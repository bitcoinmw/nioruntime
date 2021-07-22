// Copyright 2021 The BMW Developers
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::duration_to_timespec;
use crate::util::threadpool::ThreadPool;
use crate::util::{Error, ErrorKind};
use kqueue_sys::EventFilter;
use kqueue_sys::EventFlag;
use kqueue_sys::FilterFlag;
use kqueue_sys::{kevent, kqueue};
use libc::uintptr_t;
use nioruntime_libnio::ActionType;
use nioruntime_libnio::EventHandler;
use nix::errno::Errno;
use nix::fcntl::fcntl;
use nix::fcntl::OFlag;
use nix::fcntl::F_SETFL;
use nix::sys::socket::accept;
use nix::unistd::close;
use nix::unistd::pipe;
use nix::unistd::read;
use nix::unistd::write;
use std::os::unix::io::RawFd;
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::time::Duration;

const INITIAL_MAX_FDS: usize = 100;

#[derive(Debug, Clone)]
struct RawFdAction {
	fd: RawFd,
	atype: ActionType,
}

impl RawFdAction {
	fn new(fd: RawFd, atype: ActionType) -> RawFdAction {
		RawFdAction { fd, atype }
	}
}

enum HandlerEventType {
	Accept,
	Close,
	Resume,
}

struct HandlerEvent {
	etype: HandlerEventType,
	fd: RawFd,
}

impl HandlerEvent {
	fn new(fd: RawFd, etype: HandlerEventType) -> Self {
		HandlerEvent { fd, etype }
	}
}

#[derive(Debug, PartialEq)]
enum FdType {
	Wakeup,
	Listener,
	Stream,
	PausedStream,
	Unknown,
}

pub struct KqueueEventHandler {
	proc_list: Arc<Mutex<(Vec<RawFdAction>, RawFd, bool)>>,
}

impl EventHandler for KqueueEventHandler {
	fn add_fd(&mut self, fd: RawFd, atype: ActionType) -> Result<i32, Error> {
		let mut proc_list = self.proc_list.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("{}", e)).into();
			error
		})?;
		proc_list.0.push(RawFdAction::new(fd, atype));
		Ok(fd.into())
	}

	fn remove_fd(&mut self, fd: RawFd) -> Result<(), Error> {
		let mut proc_list = self.proc_list.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("{}", e)).into();
			error
		})?;
		proc_list.0.push(RawFdAction::new(fd, ActionType::Remove));
		Ok(())
	}
}

impl KqueueEventHandler {
	fn do_wakeup(proc_list: &Arc<Mutex<(Vec<RawFdAction>, RawFd, bool)>>) -> Result<(), Error> {
		let (fd, wakeup_scheduled) = {
			let mut proc_list = proc_list.lock().map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("{}", e)).into();
				error
			})?;
			let wakeup_scheduled = proc_list.2;
			if !wakeup_scheduled {
				proc_list.2 = true;
			}
			(proc_list.1, wakeup_scheduled)
		};

		if !wakeup_scheduled {
			write(fd, &[0u8; 1])?;
		}
		Ok(())
	}

	pub fn wakeup(&mut self) -> Result<(), Error> {
		Self::do_wakeup(&self.proc_list)?;
		Ok(())
	}

	pub fn new() -> Result<KqueueEventHandler, Error> {
		// create the kqueue
		let queue = unsafe { kqueue() };

		// create the pipe (for wakeups)
		let (rx, tx) = pipe()?;

		if (queue as i32) < 0 {
			// OS Level error (no fds available?)
			return Err(ErrorKind::InternalError("could not create kqueue".to_string()).into());
		}

		let proc_list = Arc::new(Mutex::new((vec![], tx, false)));
		let cloned_proc_list = proc_list.clone();

		spawn(move || {
			let res = Self::poll_loop(&cloned_proc_list, queue, rx);
			match res {
				Ok(_) => {
					println!("poll_loop exited normally");
				}
				Err(e) => {
					println!("FATAL: Unexpected error in poll loop: {}", e.to_string());
				}
			}
		});

		Ok(KqueueEventHandler { proc_list })
	}

	fn poll_loop(
		proc_list: &Arc<Mutex<(Vec<RawFdAction>, RawFd, bool)>>,
		queue: RawFd,
		rx: RawFd,
	) -> Result<(), Error> {
		let thread_pool = ThreadPool::new(4)?;
		let handler_queue: Vec<HandlerEvent> = vec![];
		let handler_queue = Arc::new(Mutex::new(handler_queue));

		let mut info_holder = Vec::new();
		// preallocate some
		info_holder.reserve(INITIAL_MAX_FDS);
		for _ in 0..INITIAL_MAX_FDS {
			info_holder.push(FdType::Unknown);
		}
		info_holder[rx as usize] = FdType::Wakeup;

		let mut ret_kev = kevent::new(
			0,
			EventFilter::EVFILT_SYSCOUNT,
			EventFlag::empty(),
			FilterFlag::empty(),
		);

		// add the wakeup pipe rx here
		let mut kevs: Vec<kevent> = Vec::new();
		let rx = rx as uintptr_t;
		kevs.push(kevent::new(
			rx,
			EventFilter::EVFILT_READ,
			EventFlag::EV_ADD,
			FilterFlag::empty(),
		));

		unsafe {
			kevent(
				queue,
				kevs.as_ptr(),
				kevs.len() as i32,
				&mut ret_kev,
				1,
				&duration_to_timespec(Duration::from_millis(1)),
			)
		};

		println!("Server started");
		loop {
			let to_process;
			{
				let mut proc_list = proc_list.lock().map_err(|e| {
					let error: Error = ErrorKind::InternalError(format!("{}", e)).into();
					error
				})?;
				to_process = proc_list.0.clone();
				proc_list.0.clear();
			}

			let mut kevs: Vec<kevent> = Vec::new();

			for proc in to_process {
				match proc.atype {
					ActionType::AddStream => {
						let fd = proc.fd as uintptr_t;
						kevs.push(kevent::new(
							fd,
							EventFilter::EVFILT_READ,
							EventFlag::EV_ADD,
							FilterFlag::empty(),
						));

						// make sure there's enough space
						let len = info_holder.len();
						if fd >= len {
							for _ in len..fd + 1 {
								info_holder.push(FdType::Unknown);
							}
						}
						info_holder[fd] = FdType::Stream;
					}
					ActionType::AddListener => {
						let fd = proc.fd as uintptr_t;
						kevs.push(kevent::new(
							fd,
							EventFilter::EVFILT_READ,
							EventFlag::EV_ADD,
							FilterFlag::empty(),
						));

						// make sure there's enough space
						let len = info_holder.len();
						if fd >= len {
							for _ in len..fd + 1 {
								info_holder.push(FdType::Unknown);
							}
						}
						info_holder[fd] = FdType::Listener;
					}
					ActionType::Remove => {}
				}
			}
			// check if we accepted a connection
			{
				let mut handler_queue = handler_queue.lock().map_err(|e| {
					let error: Error =
						ErrorKind::InternalError(format!("Poison error: {}", e)).into();
					error
				})?;

				let last_fd = ret_kev.ident as uintptr_t;
				let mut resume_collision = false;
				for handler_event in &*handler_queue {
					match handler_event.etype {
						HandlerEventType::Accept => {
							let fd = handler_event.fd as uintptr_t;
							kevs.push(kevent::new(
								fd,
								EventFilter::EVFILT_READ,
								EventFlag::EV_ADD,
								FilterFlag::empty(),
							));
							// make sure there's enough space
							let len = info_holder.len();
							if fd >= len {
								for _ in len..fd + 1 {
									info_holder.push(FdType::Unknown);
								}
							}
							info_holder[fd] = FdType::Stream;
						}
						HandlerEventType::Close => {
							let fd = handler_event.fd as uintptr_t;
							kevs.push(kevent::new(
								fd,
								EventFilter::EVFILT_READ,
								EventFlag::EV_DELETE,
								FilterFlag::empty(),
							));
							info_holder[handler_event.fd as usize] = FdType::Unknown;
						}
						HandlerEventType::Resume => {
							let fd = handler_event.fd as uintptr_t;
							if last_fd != fd {
								kevs.push(kevent::new(
									fd,
									EventFilter::EVFILT_READ,
									EventFlag::EV_ADD,
									FilterFlag::empty(),
								));
								let len = info_holder.len();
								if fd >= len {
									for _ in len..fd + 1 {
										info_holder.push(FdType::Unknown);
									}
								}
								info_holder[fd] = FdType::Stream;
							} else {
								resume_collision = true;
							}
						}
					}
				}
				(*handler_queue).clear();

				if !resume_collision
					&& last_fd != 0 && info_holder[last_fd as usize] == FdType::Stream
				{
					kevs.push(kevent::new(
						last_fd,
						EventFilter::EVFILT_READ,
						EventFlag::EV_DELETE,
						FilterFlag::empty(),
					));
					info_holder[last_fd] = FdType::PausedStream;
				}
			}
			ret_kev = kevent::new(
				0,
				EventFilter::EVFILT_SYSCOUNT,
				EventFlag::empty(),
				FilterFlag::empty(),
			);

			let ret = unsafe {
				kevent(
					queue,
					kevs.as_ptr(),
					kevs.len() as i32,
					&mut ret_kev,
					1, // TODO: can we process more than one event?
					&duration_to_timespec(Duration::from_millis(100)),
				)
			};

			if ret == 0 || ret_kev.flags.contains(EventFlag::EV_DELETE) {
				continue;
			}

			let res = match ret {
				-1 => Self::process_error(ret_kev, &info_holder),
				_ => Self::process_event(
					ret_kev,
					&info_holder,
					&thread_pool,
					&handler_queue,
					proc_list,
				),
			};

			match res {
				Ok(_) => {}
				Err(e) => {
					println!("Unexpected error in poll loop: {}", e.to_string());
				}
			}
		}
	}

	fn process_error(kev: kevent, info_holder: &Vec<FdType>) -> Result<(), Error> {
		let fd = kev.ident as RawFd;
		let fd_type = &info_holder[fd as usize];
		println!("Error on fd = {}, type = {:?}", fd, fd_type);
		Ok(())
	}

	fn process_event(
		kev: kevent,
		info_holder: &Vec<FdType>,
		thread_pool: &ThreadPool,
		handler_queue: &Arc<Mutex<Vec<HandlerEvent>>>,
		proc_list: &Arc<Mutex<(Vec<RawFdAction>, RawFd, bool)>>,
	) -> Result<(), Error> {
		let fd = kev.ident as RawFd;
		let fd_type = &info_holder[fd as usize];
		match fd_type {
			FdType::Listener => {
				// one at a time per fd
				let res = accept(fd);
				match res {
					Ok(res) => {
						// set non-blocking
						fcntl(res, F_SETFL(OFlag::from_bits(libc::O_NONBLOCK).unwrap()))?;
						Self::process_accept_result(fd, res, handler_queue)
					}
					Err(e) => Self::process_accept_err(fd, e),
				}?;
			}
			FdType::Stream => {
				let handler_queue = handler_queue.clone();
				let proc_list = proc_list.clone();
				thread_pool.execute(async move {
					let mut buf = [0u8; 100];
					// in order to ensure sequence in tact, obtain the lock
					let res = read(fd, &mut buf);
					let _ = match res {
						Ok(res) => {
							Self::process_read_result(fd, res, buf, &handler_queue, &proc_list)
						}
						Err(e) => Self::process_read_err(fd, e, &handler_queue),
					};
				})?;
			}
			FdType::Unknown => {
				println!("unexpected fd_type (unknown) for fd: {}", fd);
			}
			FdType::PausedStream => {
				println!("unexpected fd_type (paused stream) for fd: {}", fd);
			}
			FdType::Wakeup => {
				read(fd, &mut [0u8; 1])?;
				let mut proc_list = proc_list.lock().map_err(|e| {
					let error: Error = ErrorKind::InternalError(format!("{}", e)).into();
					error
				})?;
				proc_list.2 = false;
			}
		}

		Ok(())
	}

	fn process_read_result(
		fd: RawFd,
		len: usize,
		buf: [u8; 100],
		handler_queue: &Arc<Mutex<Vec<HandlerEvent>>>,
		proc_list: &Arc<Mutex<(Vec<RawFdAction>, RawFd, bool)>>,
	) -> Result<(), Error> {
		if len > 0 {
			let utf8_ver = std::str::from_utf8(&buf[0..len as usize]);
			match utf8_ver {
				Ok(s) => {
					println!("read {} bytes on fd = {} = '{}'", len, fd, s);
				}
				Err(_e) => {
					println!("{} binary bytes of data on fd = {}", len, fd)
				}
			}
			Self::push_handler_queue(fd, HandlerEventType::Resume, handler_queue)?;
			Self::do_wakeup(proc_list)?;
		} else {
			println!("read 0 bytes EOF closing fd = {}", fd);
			// close
			Self::push_handler_queue(fd, HandlerEventType::Close, handler_queue)?;
			let _ = close(fd);
		}
		Ok(())
	}

	fn process_read_err(
		fd: RawFd,
		error: Errno,
		handler_queue: &Arc<Mutex<Vec<HandlerEvent>>>,
	) -> Result<(), Error> {
		// don't close if it's an EAGAIN or one of the other non-terminal errors
		match error {
			Errno::EAGAIN => {
				Self::push_handler_queue(fd, HandlerEventType::Resume, handler_queue)?;
			}
			_ => {
				println!("closing with error fd = {}", fd);
				Self::push_handler_queue(fd, HandlerEventType::Close, handler_queue)?;
				let _ = close(fd);
			}
		}
		println!("read error on {}, error: {}", fd, error);
		Ok(())
	}

	fn process_accept_result(
		_acceptor: RawFd,
		nfd: RawFd,
		handler_queue: &Arc<Mutex<Vec<HandlerEvent>>>,
	) -> Result<(), Error> {
		println!("accepted fd = {}", nfd);
		Self::push_handler_queue(nfd, HandlerEventType::Accept, handler_queue)?;
		Ok(())
	}

	fn process_accept_err(_acceptor: RawFd, error: Errno) -> Result<(), Error> {
		println!("error on acceptor: {}", error);
		Ok(())
	}

	fn push_handler_queue(
		fd: RawFd,
		event_type: HandlerEventType,
		handler_queue: &Arc<Mutex<Vec<HandlerEvent>>>,
	) -> Result<(), Error> {
		let handler_queue = handler_queue.lock();
		match handler_queue {
			Ok(mut handler_queue) => {
				handler_queue.push(HandlerEvent::new(fd, event_type));
			}
			Err(e) => {
				println!("Unexpected handler error: {}", e.to_string());
			}
		}
		Ok(())
	}
}

#[test]
fn test_kqueues() -> Result<(), Error> {
	use std::net::TcpListener;

	let listener = TcpListener::bind("127.0.0.1:9999")?;
	let mut kqe = KqueueEventHandler::new()?;
	kqe.add_tcp_listener(&listener)?;
	std::thread::sleep(std::time::Duration::from_millis(1000));
	Ok(())
}
