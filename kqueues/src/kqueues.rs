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
use crate::util::{Error, ErrorKind};
use kqueue_sys::EventFilter;
use kqueue_sys::EventFlag;
use kqueue_sys::FilterFlag;
use kqueue_sys::{kevent, kqueue};
use libc::uintptr_t;
use nioruntime_fdhandler::{EventType, FdHandler, ProcessEventResult, ResultType};
use nioruntime_libnio::ActionType;
use nioruntime_libnio::EventHandler;
use std::os::unix::io::RawFd;
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::time::Duration;

const INITIAL_MAX_FDS: usize = 100_000;

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

	fn setup_handlers(
		fdhandler: &mut FdHandler,
		handler_queue: Arc<Mutex<Vec<HandlerEvent>>>,
	) -> Result<(), Error> {
		fdhandler.register_on_read(move |fd, data| {
			println!("in fdhandler.onread");
			let str = std::str::from_utf8(&data[..]);
			match str {
				Ok(str) => {
					println!("read {} bytes on fd[{}] \"{}\"", data.len(), fd, str,);
				}
				Err(e) => {
					println!("read {} bytes of binary data on fd[{}]", data.len(), fd,);
				}
			}
		})?;

		let handler_queue_clone = handler_queue.clone();
		fdhandler.register_on_close(move |fd| {
			println!("on close handler: {}", fd);
			let mut handler_queue = handler_queue.lock();
			match handler_queue {
				Ok(mut handler_queue) => {
					handler_queue.push(HandlerEvent::new(fd, HandlerEventType::Close));
				}
				Err(e) => {
					println!("Unexpected handler error: {}", e.to_string());
				}
			}
		})?;

		fdhandler.register_on_accept(move |fd| {
			println!("on accept handler: {}", fd);
			let mut handler_queue = handler_queue_clone.lock();
			match handler_queue {
				Ok(mut handler_queue) => {
					handler_queue.push(HandlerEvent::new(fd, HandlerEventType::Accept));
				}
				Err(e) => {
					println!("Unexpected handler error: {}", e.to_string());
				}
			}
		})?;

		Ok(())
	}

	fn poll_loop(proc_list: &Arc<Mutex<Vec<RawFdAction>>>, queue: RawFd) -> Result<(), Error> {
		let mut fdhandler = FdHandler::new(4)?;
		let handler_queue: Vec<HandlerEvent> = vec![];
		let handler_queue = Arc::new(Mutex::new(handler_queue));

		Self::setup_handlers(&mut fdhandler, handler_queue.clone())?;

		let mut info_holder = Vec::new();
		// preallocate what will most likely be enough forever
		info_holder.reserve(INITIAL_MAX_FDS);
		for _ in 0..INITIAL_MAX_FDS {
			info_holder.push(FdType::Unknown);
		}

		let mut event_result: Option<ProcessEventResult> = None;
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
						// most likely allocations will not be needed
						// due to the large initial allocation
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
									info_holder.push(FdType::Unknown);
								}
							}
							info_holder[fd] = FdType::Stream;
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
							info_holder[handler_event.fd as usize] = FdType::Unknown;
						}
					}
				}
				(*handler_queue).clear();
			}
			/*
						match event_result.as_ref() {
							Some(result) => {
								match result.result_type {
									ResultType::AcceptedFd => {
										match result.fd {
											Some(fd) => {
												let fd = fd as uintptr_t;
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
														info_holder.push(FdType::Unknown);
													}
												}
												info_holder[fd] = FdType::Stream;
											}
											None => println!("unexpected error: no fd (accept)"),
										}
									}
									ResultType::CloseFd => match result.fd {
										Some(fd) => {
											info_holder[fd as usize] = FdType::Unknown;
										}
										None => println!("unexpected error: no fd (close)"),
									},
									_ => {}
								}
							}
							None => {}
						}
			*/
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

			match ret {
				-1 => Self::process_error(ret_kev, &fdhandler, &info_holder),
				_ => Self::process_event(ret_kev, &fdhandler, &info_holder),
			};
		}
	}

	fn process_error(
		kev: kevent,
		fdhandler: &FdHandler,
		info_holder: &Vec<FdType>,
	) -> Result<(), Error> {
		let fd = kev.ident as RawFd;
		let fd_type = &info_holder[fd as usize];
		println!("Error on fd = {}, type = {:?}", fd, fd_type);
		fdhandler.process_fd_event(fd, EventType::Error)
	}

	fn process_event(
		kev: kevent,
		fdhandler: &FdHandler,
		info_holder: &Vec<FdType>,
	) -> Result<(), Error> {
		let fd = kev.ident as RawFd;
		let fd_type = &info_holder[fd as usize];
		let result = match fd_type {
			FdType::Listener => fdhandler.process_fd_event(fd, EventType::Accept)?,
			FdType::Stream => fdhandler.process_fd_event(fd, EventType::Read)?,
			FdType::Unknown => {
				return Err(
					ErrorKind::InternalError(format!("unexpected fd_type for fd: {}", fd)).into(),
				);
			}
		};

		Ok(result)
	}
}

#[test]
fn test_kqueues() -> Result<(), Error> {
	use std::net::TcpListener;

	let listener = TcpListener::bind("127.0.0.1:9999")?;
	let mut kqe = KqueueEventHandler::new()?;
	kqe.add_tcp_listener(&listener)?;
	std::thread::sleep(std::time::Duration::from_millis(1000));
	//assert!(false);
	Ok(())
}
