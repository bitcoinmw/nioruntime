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
use nix::unistd::read;
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

#[derive(Debug)]
enum FdType {
	Listener,
	Stream,
	Unknown,
}

pub struct KqueueEventHandler {
	proc_list: Arc<Mutex<Vec<RawFdAction>>>,
}

impl EventHandler for KqueueEventHandler {
	fn add_fd(&mut self, fd: RawFd, atype: ActionType) -> Result<i32, Error> {
		let mut proc_list = self.proc_list.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("{}", e)).into();
			error
		})?;
		proc_list.push(RawFdAction::new(fd, atype));
		Ok(fd.into())
	}

	fn remove_fd(&mut self, fd: RawFd) -> Result<(), Error> {
		let mut proc_list = self.proc_list.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("{}", e)).into();
			error
		})?;
		proc_list.push(RawFdAction::new(fd, ActionType::Remove));
		Ok(())
	}
}

impl KqueueEventHandler {
	pub fn new() -> Result<KqueueEventHandler, Error> {
		let queue = unsafe { kqueue() };
		if (queue as i32) < 0 {
			// OS Level error (no fds available?
			return Err(ErrorKind::InternalError("could not create kqueue".to_string()).into());
		}

		let proc_list = Arc::new(Mutex::new(vec![]));
		let cloned_proc_list = proc_list.clone();

		spawn(move || {
			let res = Self::poll_loop(&cloned_proc_list, queue);
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

	fn poll_loop(proc_list: &Arc<Mutex<Vec<RawFdAction>>>, queue: RawFd) -> Result<(), Error> {
		let thread_pool = ThreadPool::new(4)?;
		let handler_queue: Vec<HandlerEvent> = vec![];
		let handler_queue = Arc::new(Mutex::new(handler_queue));

		let mut info_holder = Vec::new();
		// preallocate some
		info_holder.reserve(INITIAL_MAX_FDS);
		for _ in 0..INITIAL_MAX_FDS {
			info_holder.push((FdType::Unknown, Arc::new(Mutex::new(false))));
		}

		println!("Server started");
		loop {
			let to_process;
			{
				let mut proc_list = proc_list
					.lock()
					.map_err(|e| {
						let error: Error = ErrorKind::InternalError(format!("{}", e)).into();
						error
					})
					.unwrap();
				to_process = proc_list.clone();
				proc_list.clear();
			}

			// give up time slice here
			// so workers can complete first
			// this reduces the "EAGAIN" from happening with read
			// happening in multiple threads
			std::thread::yield_now();

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
						// most likely allocations will not be needed
						// due to the large initial allocation
						let len = info_holder.len();
						if fd >= len {
							for _ in len..fd + 1 {
								info_holder.push((FdType::Unknown, Arc::new(Mutex::new(false))));
							}
						}
						info_holder[fd] = (FdType::Stream, Arc::new(Mutex::new(false)));
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
						// most likely allocations will not be needed
						// due to the large initial allocation
						let len = info_holder.len();
						if fd >= len {
							for _ in len..fd + 1 {
								info_holder.push((FdType::Unknown, Arc::new(Mutex::new(false))));
							}
						}
						info_holder[fd] = (FdType::Listener, Arc::new(Mutex::new(false)));
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
							// most likely allocations will not be needed
							// due to the large initial allocation
							let len = info_holder.len();
							if fd >= len {
								for _ in len..fd + 1 {
									info_holder
										.push((FdType::Unknown, Arc::new(Mutex::new(false))));
								}
							}
							info_holder[fd] = (FdType::Stream, Arc::new(Mutex::new(false)));
						}
						HandlerEventType::Close => {
							println!("closing at poll loop fd: {}", handler_event.fd);
							let fd = handler_event.fd as uintptr_t;
							kevs.push(kevent::new(
								fd,
								EventFilter::EVFILT_READ,
								EventFlag::EV_DELETE,
								FilterFlag::empty(),
							));
							info_holder[handler_event.fd as usize] =
								(FdType::Unknown, Arc::new(Mutex::new(false)));
						}
					}
				}
				(*handler_queue).clear();
			}
			let mut ret_kev = kevent::new(
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

			if ret == 0 {
				continue;
			}

			let res = match ret {
				-1 => Self::process_error(ret_kev, &info_holder),
				_ => Self::process_event(ret_kev, &info_holder, &thread_pool, &handler_queue),
			};

			match res {
				Ok(_) => {}
				Err(e) => {
					println!("Unexpected error in poll loop: {}", e.to_string());
				}
			}
		}
	}

	fn process_error(
		kev: kevent,
		info_holder: &Vec<(FdType, Arc<Mutex<bool>>)>,
	) -> Result<(), Error> {
		let fd = kev.ident as RawFd;
		let fd_type = &info_holder[fd as usize].0;
		println!("Error on fd = {}, type = {:?}", fd, fd_type);
		Ok(())
	}

	fn process_event(
		kev: kevent,
		info_holder: &Vec<(FdType, Arc<Mutex<bool>>)>,
		thread_pool: &ThreadPool,
		handler_queue: &Arc<Mutex<Vec<HandlerEvent>>>,
	) -> Result<(), Error> {
		let fd = kev.ident as RawFd;
		let fd_type = &info_holder[fd as usize].0;
		let lock = &info_holder[fd as usize].1;
		let lock = lock.clone();

		match fd_type {
			FdType::Listener => {
				println!("about to accept {}", fd);
				// one at a time per fd
				let _lock = lock.lock().unwrap();
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
				thread_pool.execute(async move {
					let mut buf = [0u8; 100];
					// in order to ensure sequence in tact, obtain the lock
					let _lock = lock.lock().unwrap();
					let res = read(fd, &mut buf);
					let _ = match res {
						Ok(res) => Self::process_read_result(fd, res, buf, handler_queue),
						Err(e) => Self::process_read_err(fd, e, &handler_queue),
					};
				})?;
			}
			FdType::Unknown => {
				println!("unexpected fd_type for fd: {}", fd);
			}
		};

		Ok(())
	}

	fn process_read_result(
		fd: RawFd,
		len: usize,
		buf: [u8; 100],
		handler_queue: Arc<Mutex<Vec<HandlerEvent>>>,
	) -> Result<(), Error> {
		println!("read len = {}", len);
		if len > 0 {
			let utf8_ver = std::str::from_utf8(&buf[0..len as usize]);
			match utf8_ver {
				Ok(s) => {
					println!("read {} bytes = '{}'", len, s,);
				}
				Err(_e) => {
					println!("{} binary bytes of data", len)
				}
			}
		} else {
			println!("read 0 bytes");
			// close
			let handler_queue = handler_queue.lock();
			match handler_queue {
				Ok(mut handler_queue) => {
					handler_queue.push(HandlerEvent::new(fd, HandlerEventType::Close));
				}
				Err(e) => {
					println!("Unexpected handler error: {}", e.to_string());
				}
			}
			close(fd)?;
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
			Errno::EAGAIN => {}
			_ => {
				let handler_queue = handler_queue.lock();
				match handler_queue {
					Ok(mut handler_queue) => {
						handler_queue.push(HandlerEvent::new(fd, HandlerEventType::Close));
					}
					Err(e) => {
						println!("Unexpected handler error: {}", e.to_string());
					}
				}
				close(fd)?;
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
		println!("res was {}", nfd);
		{
			let handler_queue = handler_queue.lock();
			match handler_queue {
				Ok(mut handler_queue) => {
					handler_queue.push(HandlerEvent::new(nfd, HandlerEventType::Accept));
				}
				Err(e) => {
					println!("Unexpected handler error: {}", e.to_string());
				}
			}
		}
		Ok(())
	}

	fn process_accept_err(_acceptor: RawFd, error: Errno) -> Result<(), Error> {
		println!("error on acceptor: {}", error);
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
