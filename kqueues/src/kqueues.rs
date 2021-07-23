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
use kqueue_sys::EventFilter::EVFILT_READ;
use kqueue_sys::EventFilter::EVFILT_WRITE;
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
use std::collections::LinkedList;
use std::os::unix::io::RawFd;
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::time::Duration;

const INITIAL_MAX_FDS: usize = 100;
const BUFFER_SIZE: usize = 1024;

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

#[derive(Clone)]
enum HandlerEventType {
	Accept,
	Close,
	Resume,
	ResumeWrite,
}

#[derive(Clone)]
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

struct WriteBuffer {
	offset: u16,
	len: u16,
	buffer: [u8; BUFFER_SIZE],
}

struct OnRead<F> {
	on_read: F,
}

impl<F> OnRead<F> where F: Fn(&[u8], usize) -> (&[u8], usize, usize) + Send + 'static + Clone + Sync {}

struct Callbacks<F> {
	on_read: Option<OnRead<F>>,
	//on_accept: Option<Box<dyn Fn(i32) -> () + Send + 'static>>,
	//on_close: Option<Box<dyn Fn(i32, &[u8]) -> () + Send + 'static>>,
	//on_write_success: Option<Box<dyn Fn(i32, u64) -> () + Send + 'static>>,
	//on_write_fail: Option<Box<dyn Fn(i32, u64) -> () + Send + 'static>>,
}

struct GuardedData<F> {
	fd_actions: Vec<RawFdAction>,
	wakeup_fd: RawFd,
	wakeup_scheduled: bool,
	handler_events: Vec<HandlerEvent>,
	write_pending: Vec<RawFd>,
	write_buffers: Vec<LinkedList<WriteBuffer>>,
	callbacks: Callbacks<F>,
}

pub struct KqueueEventHandler<F> {
	data: Arc<Mutex<GuardedData<F>>>,
}

impl<F> EventHandler for KqueueEventHandler<F> {
	fn add_fd(&mut self, fd: RawFd, atype: ActionType) -> Result<i32, Error> {
		let mut data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;
		let fd_actions = &mut data.fd_actions;
		fd_actions.push(RawFdAction::new(fd, atype));
		Ok(fd.into())
	}

	fn remove_fd(&mut self, fd: RawFd) -> Result<(), Error> {
		let mut data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;
		let fd_actions = &mut data.fd_actions;
		fd_actions.push(RawFdAction::new(fd, ActionType::Remove));
		Ok(())
	}
}

impl<F> KqueueEventHandler<F>
where
	F: Fn(&[u8], usize) -> (&[u8], usize, usize) + Send + 'static + Clone + Sync,
{
	fn do_wakeup_with_lock(data: &mut GuardedData<F>) -> Result<(RawFd, bool), Error> {
		let wakeup_scheduled = data.wakeup_scheduled;
		if !wakeup_scheduled {
			data.wakeup_scheduled = true;
		}
		Ok((data.wakeup_fd, wakeup_scheduled))
	}

	fn do_wakeup(data: &Arc<Mutex<GuardedData<F>>>) -> Result<(), Error> {
		let mut data = data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;
		let (fd, wakeup_scheduled) = Self::do_wakeup_with_lock(&mut *data)?;

		if !wakeup_scheduled {
			write(fd, &[0u8; 1])?;
		}
		Ok(())
	}

	pub fn wakeup(&mut self) -> Result<(), Error> {
		Self::do_wakeup(&self.data)?;
		Ok(())
	}

	fn write(
		id: i32,
		data: &[u8],
		offset: usize,
		len: usize,
		guarded_data: &Arc<Mutex<GuardedData<F>>>,
	) -> Result<(), Error> {
		if len + offset > data.len() {
			return Err(ErrorKind::ArrayIndexOutofBounds(format!(
				"offset+len='{}',data.len='{}'",
				offset + len,
				data.len()
			))
			.into());
		}

		// update GuardedData with our write_buffers, notification message, and wakeup
		let id_idx = id as uintptr_t;
		let (fd, wakeup_scheduled) = {
			let mut guarded_data = guarded_data.lock().map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
				error
			})?;

			let mut rem = len;
			let mut count = 0;
			loop {
				let len = match rem <= BUFFER_SIZE {
					true => rem,
					false => BUFFER_SIZE,
				} as u16;
				let mut write_buffer = WriteBuffer {
					offset: 0,
					len,
					buffer: [0u8; BUFFER_SIZE],
				};

				let start = offset + count * BUFFER_SIZE;
				let end = offset + count * BUFFER_SIZE + (len as usize);
				write_buffer.buffer[0..(len as usize)].copy_from_slice(&data[start..end]);

				let cur_len = guarded_data.write_buffers.len();
				if guarded_data.write_buffers.len() <= id_idx {
					for _ in cur_len..(id_idx + 1) {
						guarded_data.write_buffers.push(LinkedList::new());
					}
				}
				guarded_data.write_buffers[id_idx].push_back(write_buffer);
				if rem <= BUFFER_SIZE {
					break;
				}
				rem -= BUFFER_SIZE;
				count += 1;
			}

			guarded_data.write_pending.push(id.into());
			Self::do_wakeup_with_lock(&mut *guarded_data)?
		};

		if !wakeup_scheduled {
			write(fd, &[0u8; 1])?;
		}

		Ok(())
	}

	pub fn set_on_read(&mut self, on_read: F) -> Result<(), Error> {
		let mut guarded_data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		guarded_data.callbacks.on_read = Some(OnRead { on_read });

		Ok(())
	}

	pub fn new() -> Self {
		// create the kqueue
		let queue = unsafe { kqueue() };

		// create the pipe (for wakeups)
		let (rx, tx) = pipe().unwrap();

		let callbacks = Callbacks {
			//on_read: OnRead { on_read },
			on_read: None,
			//on_accept: None,
			//on_close: None,
			//on_write_success: None,
			//on_write_fail: None,
		};

		let guarded_data = GuardedData {
			fd_actions: vec![],
			wakeup_fd: tx,
			wakeup_scheduled: false,
			handler_events: vec![],
			write_pending: vec![],
			write_buffers: vec![LinkedList::new()],
			callbacks,
		};
		let write_buffers = vec![Arc::new(Mutex::new(LinkedList::new()))];
		let guarded_data = Arc::new(Mutex::new(guarded_data));
		let cloned_guarded_data = guarded_data.clone();
		let mut cloned_write_buffers = write_buffers.clone();

		spawn(move || {
			let res = Self::poll_loop(&cloned_guarded_data, &mut cloned_write_buffers, queue, rx);
			match res {
				Ok(_) => {
					println!("poll_loop exited normally");
				}
				Err(e) => {
					println!("FATAL: Unexpected error in poll loop: {}", e.to_string());
				}
			}
		});

		KqueueEventHandler { data: guarded_data }
	}

	fn poll_loop(
		guarded_data: &Arc<Mutex<GuardedData<F>>>,
		write_buffers: &mut Vec<Arc<Mutex<LinkedList<WriteBuffer>>>>,
		queue: RawFd,
		rx: RawFd,
	) -> Result<(), Error> {
		let thread_pool = ThreadPool::new(4)?;

		let mut read_fd_type = Vec::new();
		// preallocate some
		read_fd_type.reserve(INITIAL_MAX_FDS);
		for _ in 0..INITIAL_MAX_FDS {
			read_fd_type.push(FdType::Unknown);
		}
		read_fd_type[rx as usize] = FdType::Wakeup;

		// same for write_buffers
		for _ in 0..INITIAL_MAX_FDS {
			write_buffers.push(Arc::new(Mutex::new(LinkedList::new())));
		}

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
			let handler_events;
			let write_pending;
			let on_read;
			{
				let mut guarded_data = guarded_data.lock().map_err(|e| {
					let error: Error =
						ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
					error
				})?;
				to_process = guarded_data.fd_actions.clone();
				handler_events = guarded_data.handler_events.clone();
				write_pending = guarded_data.write_pending.clone();
				on_read = guarded_data
					.callbacks
					.on_read
					.as_ref()
					.unwrap()
					.on_read
					.clone();
				guarded_data.fd_actions.clear();
				guarded_data.handler_events.clear();
				guarded_data.write_pending.clear();
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
						let len = read_fd_type.len();
						if fd >= len {
							for _ in len..fd + 1 {
								read_fd_type.push(FdType::Unknown);
							}
						}
						read_fd_type[fd] = FdType::Stream;
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
						let len = read_fd_type.len();
						if fd >= len {
							for _ in len..fd + 1 {
								read_fd_type.push(FdType::Unknown);
							}
						}
						read_fd_type[fd] = FdType::Listener;
					}
					ActionType::Remove => {
						let fd = proc.fd as uintptr_t;
						kevs.push(kevent::new(
							fd,
							EventFilter::EVFILT_READ,
							EventFlag::EV_DELETE,
							FilterFlag::empty(),
						));

						// make sure there's enough space
						let len = read_fd_type.len();
						if fd >= len {
							for _ in len..fd + 1 {
								read_fd_type.push(FdType::Unknown);
							}
						}
						read_fd_type[fd] = FdType::Unknown;
					}
				}
			}

			// check if we accepted a connection
			let last_fd = ret_kev.ident as uintptr_t;
			let mut resume_collision = false;
			for handler_event in handler_events {
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
						let len = read_fd_type.len();
						if fd >= len {
							for _ in len..fd + 1 {
								read_fd_type.push(FdType::Unknown);
							}
						}
						read_fd_type[fd] = FdType::Stream;
					}
					HandlerEventType::Close => {
						let fd = handler_event.fd as uintptr_t;
						kevs.push(kevent::new(
							fd,
							EventFilter::EVFILT_READ,
							EventFlag::EV_DELETE,
							FilterFlag::empty(),
						));
						read_fd_type[handler_event.fd as usize] = FdType::Unknown;

						// delete any unwritten buffers
						if fd < write_buffers.len() {
							let mut linked_list =
								write_buffers[fd as usize].lock().map_err(|e| {
									let error: Error =
										ErrorKind::InternalError(format!("Poison Error: {}", e))
											.into();
									error
								})?;
							(*linked_list).clear();
						}
					}
					HandlerEventType::Resume => {
						let fd = handler_event.fd as uintptr_t;
						if last_fd != fd || ret_kev.filter != EVFILT_READ {
							kevs.push(kevent::new(
								fd,
								EventFilter::EVFILT_READ,
								EventFlag::EV_ADD,
								FilterFlag::empty(),
							));
							let len = read_fd_type.len();
							if fd >= len {
								for _ in len..fd + 1 {
									read_fd_type.push(FdType::Unknown);
								}
							}
							read_fd_type[fd] = FdType::Stream;
						} else {
							resume_collision = true;
						}
					}
					HandlerEventType::ResumeWrite => {
						let fd = handler_event.fd as uintptr_t;
						if last_fd != fd || ret_kev.filter != EVFILT_WRITE {
							kevs.push(kevent::new(
								fd,
								EventFilter::EVFILT_WRITE,
								EventFlag::EV_ADD,
								FilterFlag::empty(),
							));
						} else {
							resume_collision = true;
						}
					}
				}
			}

			if !resume_collision
				&& last_fd != 0 && read_fd_type[last_fd as usize] == FdType::Stream
				&& ret_kev.filter == EVFILT_READ
			{
				println!(
					"pushing disabling events: {:?}",
					read_fd_type[last_fd as usize]
				);
				kevs.push(kevent::new(
					last_fd,
					EventFilter::EVFILT_READ,
					EventFlag::EV_DELETE,
					FilterFlag::empty(),
				));
				read_fd_type[last_fd] = FdType::PausedStream;
			}

			if !resume_collision
				&& last_fd != 0 && (read_fd_type[last_fd as usize] == FdType::Stream
				|| read_fd_type[last_fd as usize] == FdType::PausedStream)
				&& ret_kev.filter == EVFILT_WRITE
			{
				kevs.push(kevent::new(
					last_fd,
					EventFilter::EVFILT_WRITE,
					EventFlag::EV_DELETE,
					FilterFlag::empty(),
				));
			}

			// handle write_pending
			for pending in write_pending {
				let pending = pending as uintptr_t;
				kevs.push(kevent::new(
					pending,
					EventFilter::EVFILT_WRITE,
					EventFlag::EV_ADD,
					FilterFlag::empty(),
				));
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
				-1 => Self::process_error(ret_kev, &read_fd_type),
				_ => Self::process_event(
					ret_kev,
					&read_fd_type,
					&thread_pool,
					guarded_data,
					write_buffers,
					on_read,
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

	fn process_error(kev: kevent, read_fd_type: &Vec<FdType>) -> Result<(), Error> {
		let fd = kev.ident as RawFd;
		let fd_type = &read_fd_type[fd as usize];
		println!("Error on fd = {}, type = {:?}", fd, fd_type);
		Ok(())
	}

	fn process_event(
		kev: kevent,
		read_fd_type: &Vec<FdType>,
		thread_pool: &ThreadPool,
		guarded_data: &Arc<Mutex<GuardedData<F>>>,
		write_buffers: &mut Vec<Arc<Mutex<LinkedList<WriteBuffer>>>>,
		on_read: F,
	) -> Result<(), Error> {
		if kev.filter == EVFILT_WRITE {
			Self::process_event_write(kev, thread_pool, write_buffers, guarded_data)?;
		}
		if kev.filter == EVFILT_READ {
			Self::process_event_read(kev, read_fd_type, thread_pool, guarded_data, on_read)?;
		}

		Ok(())
	}

	fn process_event_write(
		kev: kevent,
		thread_pool: &ThreadPool,
		write_buffers: &mut Vec<Arc<Mutex<LinkedList<WriteBuffer>>>>,
		guarded_data: &Arc<Mutex<GuardedData<F>>>,
	) -> Result<(), Error> {
		let fd = kev.ident as RawFd;
		{
			let mut guarded_data = guarded_data.lock().map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
				error
			})?;

			let cur_len = write_buffers.len();
			if cur_len <= fd as usize {
				for _ in cur_len..fd as usize {
					write_buffers.push(Arc::new(Mutex::new(LinkedList::new())));
				}
			}

			let mut linked_list = write_buffers[fd as usize].lock().map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
				error
			})?;

			while !guarded_data.write_buffers[fd as usize].is_empty() {
				match guarded_data.write_buffers[fd as usize].pop_front() {
					Some(buffer) => linked_list.push_back(buffer),
					None => println!("unexpected none!"),
				}
			}
		}

		let write_buffer_clone = write_buffers[fd as usize].clone();
		let guarded_data_clone = guarded_data.clone();

		thread_pool.execute(async move {
			let write_buffer = write_buffer_clone.lock();
			match write_buffer {
				Ok(mut linked_list) => {
					let front_mut = (*linked_list).front_mut();
					match front_mut {
						Some(front_mut) => {
							let res = write(
								fd,
								&front_mut.buffer
									[(front_mut.offset as usize)..(front_mut.len as usize)],
							);
							match res {
								Ok(len) => {
									if len == (front_mut.len - front_mut.offset) as usize {
										match (*linked_list).pop_front() {
											Some(_) => {}
											None => {
												println!("unexpected error couldn't pop");
											}
										}
										if !(*linked_list).is_empty() {
											let res = Self::push_handler_event(
												fd,
												HandlerEventType::ResumeWrite,
												&guarded_data_clone,
												true,
											);
											match res {
												Ok(_) => {}
												Err(e) => {
													println!("handler push err: {}", e.to_string())
												}
											}
										}
									} else {
										front_mut.offset += len as u16;
										let res = Self::push_handler_event(
											fd,
											HandlerEventType::ResumeWrite,
											&guarded_data_clone,
											true,
										);
										match res {
											Ok(_) => {}
											Err(e) => {
												println!("handler push err: {}", e.to_string())
											}
										}
									}
								}
								Err(e) => {
									println!("write error: {}", e.to_string());
									(*linked_list).clear();
								}
							}
						}
						None => {
							println!("unepxected none");
						}
					}
				}
				Err(e) => println!(
					"unexpected error with locking write_buffer: {}",
					e.to_string()
				),
			}
		})?;

		Ok(())
	}

	fn process_event_read(
		kev: kevent,
		read_fd_type: &Vec<FdType>,
		thread_pool: &ThreadPool,
		guarded_data: &Arc<Mutex<GuardedData<F>>>,
		on_read: F,
	) -> Result<(), Error> {
		let fd = kev.ident as RawFd;
		let fd_type = &read_fd_type[fd as usize];
		match fd_type {
			FdType::Listener => {
				// one at a time per fd
				let res = accept(fd);
				match res {
					Ok(res) => {
						// set non-blocking
						fcntl(res, F_SETFL(OFlag::from_bits(libc::O_NONBLOCK).unwrap()))?;
						Self::process_accept_result(fd, res, guarded_data)
					}
					Err(e) => Self::process_accept_err(fd, e),
				}?;
			}
			FdType::Stream => {
				let guarded_data = guarded_data.clone();
				thread_pool.execute(async move {
					let mut buf = [0u8; BUFFER_SIZE];
					// in order to ensure sequence in tact, obtain the lock
					let res = read(fd, &mut buf);
					let _ = match res {
						Ok(res) => Self::process_read_result(fd, res, buf, &guarded_data, on_read),
						Err(e) => Self::process_read_err(fd, e, &guarded_data),
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
				let mut guarded_data = guarded_data.lock().map_err(|e| {
					let error: Error =
						ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
					error
				})?;
				guarded_data.wakeup_scheduled = false;
			}
		}

		Ok(())
	}

	fn process_read_result(
		fd: RawFd,
		len: usize,
		buf: [u8; BUFFER_SIZE],
		guarded_data: &Arc<Mutex<GuardedData<F>>>,
		on_read: F,
	) -> Result<(), Error> {
		if len > 0 {
			let (resp, offset, len) = (on_read)(&buf, len);
			if len > 0 {
				Self::write(fd, resp, offset, len, guarded_data)?;
			}

			Self::push_handler_event(fd, HandlerEventType::Resume, guarded_data, true)?;
		} else {
			println!("read 0 bytes EOF closing fd = {}", fd);
			// close
			Self::push_handler_event(fd, HandlerEventType::Close, guarded_data, false)?;
			let _ = close(fd);
		}
		Ok(())
	}

	fn process_read_err(
		fd: RawFd,
		error: Errno,
		guarded_data: &Arc<Mutex<GuardedData<F>>>,
	) -> Result<(), Error> {
		// don't close if it's an EAGAIN or one of the other non-terminal errors
		match error {
			Errno::EAGAIN => {
				Self::push_handler_event(fd, HandlerEventType::Resume, guarded_data, false)?;
			}
			_ => {
				println!("closing with error fd = {}", fd);
				Self::push_handler_event(fd, HandlerEventType::Close, guarded_data, false)?;
				let _ = close(fd);
			}
		}
		println!("read error on {}, error: {}", fd, error);
		Ok(())
	}

	fn process_accept_result(
		_acceptor: RawFd,
		nfd: RawFd,
		guarded_data: &Arc<Mutex<GuardedData<F>>>,
	) -> Result<(), Error> {
		println!("accepted fd = {}", nfd);
		Self::push_handler_event(nfd, HandlerEventType::Accept, guarded_data, false)?;
		Ok(())
	}

	fn process_accept_err(_acceptor: RawFd, error: Errno) -> Result<(), Error> {
		println!("error on acceptor: {}", error);
		Ok(())
	}

	fn push_handler_event(
		fd: RawFd,
		event_type: HandlerEventType,
		guarded_data: &Arc<Mutex<GuardedData<F>>>,
		wakeup: bool,
	) -> Result<(), Error> {
		{
			let guarded_data = guarded_data.lock();
			let mut wakeup_fd = 0;
			let mut wakeup_scheduled = false;

			match guarded_data {
				Ok(mut guarded_data) => {
					guarded_data
						.handler_events
						.push(HandlerEvent::new(fd, event_type));
					if wakeup {
						wakeup_scheduled = guarded_data.wakeup_scheduled;
						if !wakeup_scheduled {
							guarded_data.wakeup_scheduled = true;
						}
						wakeup_fd = guarded_data.wakeup_fd;
					}
				}
				Err(e) => {
					println!("Unexpected handler error: {}", e.to_string());
				}
			}
			if wakeup && !wakeup_scheduled {
				write(wakeup_fd, &[0u8; 1])?;
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
