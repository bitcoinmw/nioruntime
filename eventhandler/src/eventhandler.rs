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

// macos/bsd deps
#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
use crate::duration_to_timespec;
#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
use kqueue_sys::EventFilter::{self, EVFILT_READ, EVFILT_WRITE};
#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
use kqueue_sys::{kevent, kqueue, EventFlag, FilterFlag};

// linux deps
#[cfg(target_os = "linux")]
use nix::sys::epoll::{
	epoll_create1, epoll_ctl, epoll_wait, EpollCreateFlags, EpollEvent, EpollFlags, EpollOp,
};

// unix specific deps
#[cfg(unix)]
use libc::fcntl;
#[cfg(unix)]
use libc::{close, pipe, read, write};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use crate::util::threadpool::StaticThreadPool;
use crate::util::{Error, ErrorKind};
use errno::errno;
use libc::accept;
use libc::c_int;
use libc::c_void;
use libc::uintptr_t;
use libc::EAGAIN;
use log::*;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::convert::TryInto;
use std::net::TcpListener;
use std::net::TcpStream;
#[cfg(target_os = "windows")]
use std::os::windows::io::AsRawSocket;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::spawn;
#[cfg(target_os = "windows")]
use wepoll_sys::{
	epoll_close, epoll_create, epoll_ctl, epoll_data_t, epoll_event, epoll_wait, EPOLLIN, EPOLLOUT,
	EPOLLRDHUP, EPOLL_CTL_ADD, EPOLL_CTL_DEL, EPOLL_CTL_MOD,
};

const INITIAL_MAX_FDS: usize = 100;
const BUFFER_SIZE: usize = 1024;
const MAX_EVENTS: i32 = 100;

#[derive(Debug, Clone)]
pub enum ActionType {
	AddStream,
	AddListener,
	Remove,
}

#[derive(Debug, PartialEq)]
enum GenericEventType {
	AddReadET,
	AddReadLT,
	DelRead,
	AddWriteET,
	DelWrite,
}

#[derive(Debug)]
struct GenericEvent {
	fd: i32,
	etype: GenericEventType,
}

impl GenericEvent {
	fn new(fd: i32, etype: GenericEventType) -> Self {
		GenericEvent { fd, etype }
	}

	#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
	fn to_kev(&self) -> kevent {
		kevent::new(
			self.fd as uintptr_t,
			match &self.etype {
				GenericEventType::AddReadET => EventFilter::EVFILT_READ,
				GenericEventType::AddReadLT => EventFilter::EVFILT_READ,
				GenericEventType::DelRead => EventFilter::EVFILT_READ,
				GenericEventType::AddWriteET => EventFilter::EVFILT_WRITE,
				GenericEventType::DelWrite => EventFilter::EVFILT_WRITE,
			},
			match &self.etype {
				GenericEventType::AddReadET => EventFlag::EV_ADD | EventFlag::EV_CLEAR,
				GenericEventType::AddReadLT => EventFlag::EV_ADD,
				GenericEventType::DelRead => EventFlag::EV_DELETE,
				GenericEventType::AddWriteET => EventFlag::EV_ADD | EventFlag::EV_CLEAR,
				GenericEventType::DelWrite => EventFlag::EV_DELETE,
			},
			FilterFlag::empty(),
		)
	}
}

#[derive(Clone)]
pub struct WriteHandle {
	fd: i32,
	guarded_data: Arc<Mutex<GuardedData>>,
	pub connection_id: u128,
	fd_lock: Arc<Mutex<StateInfo>>,
}

impl WriteHandle {
	pub fn new(
		fd: i32,
		guarded_data: Arc<Mutex<GuardedData>>,
		connection_id: u128,
		fd_lock: Arc<Mutex<StateInfo>>,
	) -> Self {
		WriteHandle {
			fd,
			guarded_data,
			connection_id,
			fd_lock,
		}
	}

	pub fn close(&self) -> Result<(), Error> {
		self.write(&[1], 0, 0, true)
	}

	pub fn write(&self, data: &[u8], offset: usize, len: usize, close: bool) -> Result<(), Error> {
		if len + offset > data.len() {
			return Err(ErrorKind::ArrayIndexOutofBounds(format!(
				"offset+len='{}',data.len='{}'",
				offset + len,
				data.len()
			))
			.into());
		}
		{
			let linked_list = &mut self
				.fd_lock
				.lock()
				.map_err(|e| {
					let error: Error =
						ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
					error
				})?
				.write_buffer;
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
					close: match rem <= BUFFER_SIZE {
						true => close,
						false => false,
					},
					connection_seqno: self.connection_id,
				};

				let start = offset + count * BUFFER_SIZE;
				let end = offset + count * BUFFER_SIZE + (len as usize);
				write_buffer.buffer[0..(len as usize)].copy_from_slice(&data[start..end]);

				linked_list.push_back(write_buffer);

				if rem <= BUFFER_SIZE {
					break;
				}
				rem -= BUFFER_SIZE;
				count += 1;
			}
		}

		let (fd, wakeup_scheduled) = {
			let mut guarded_data = self.guarded_data.lock().map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
				error
			})?;

			guarded_data.write_pending.push(self.fd.into());
			Self::do_wakeup_with_lock(&mut *guarded_data)?
		};

		if !wakeup_scheduled {
			#[cfg(unix)]
			{
				let buf: *mut c_void = &mut [0u8; 1] as *mut _ as *mut c_void;
				unsafe {
					write(fd, buf, 1);
				}
			}
			#[cfg(target_os = "windows")]
			{
				let buf: *mut i8 = &mut [0i8; 1] as *mut _ as *mut i8;
				unsafe {
					ws2_32::send(fd.try_into().unwrap_or(0), buf, 1, 0);
				}
			}
		}

		Ok(())
	}

	fn do_wakeup_with_lock(data: &mut GuardedData) -> Result<(i32, bool), Error> {
		let wakeup_scheduled = data.wakeup_scheduled;
		if !wakeup_scheduled {
			data.wakeup_scheduled = true;
		}
		Ok((data.wakeup_fd, wakeup_scheduled))
	}
}

#[derive(Debug, Clone)]
struct FdAction {
	fd: i32,
	atype: ActionType,
}

impl FdAction {
	fn new(fd: i32, atype: ActionType) -> FdAction {
		FdAction { fd, atype }
	}
}

#[derive(Clone, PartialEq, Debug)]
enum HandlerEventType {
	Accept,
	Close,
	#[cfg(target_os = "windows")]
	PauseRead,
	#[cfg(target_os = "windows")]
	ResumeRead,
	#[cfg(target_os = "windows")]
	PauseWrite,
	#[cfg(target_os = "windows")]
	ResumeWrite,
}

#[derive(Clone, PartialEq, Debug)]
struct HandlerEvent {
	etype: HandlerEventType,
	fd: i32,
	seqno: u128,
}

impl HandlerEvent {
	fn new(fd: i32, etype: HandlerEventType, seqno: u128) -> Self {
		HandlerEvent { fd, etype, seqno }
	}
}

#[derive(Clone, Debug, PartialEq)]
enum FdType {
	Wakeup,
	Listener,
	Stream,
	Unknown,
}

#[derive(Debug)]
pub struct WriteBuffer {
	offset: u16,
	len: u16,
	buffer: [u8; BUFFER_SIZE],
	close: bool,
	connection_seqno: u128,
}

struct Callbacks<F, G, H, K> {
	on_read: Option<Pin<Box<F>>>,
	on_accept: Option<Pin<Box<G>>>,
	on_close: Option<Pin<Box<H>>>,
	on_client_read: Option<Pin<Box<K>>>,
}

#[derive(Debug, PartialEq)]
enum State {
	Normal,
	Closing,
	Closed,
}

#[derive(Debug)]
pub struct StateInfo {
	fd: i32,
	state: State,
	seqno: u128,
	write_buffer: LinkedList<WriteBuffer>,
}

impl Default for StateInfo {
	fn default() -> Self {
		StateInfo {
			fd: 0,
			state: State::Normal,
			seqno: 0,
			write_buffer: LinkedList::new(),
		}
	}
}

impl StateInfo {
	fn new(fd: i32, state: State, seqno: u128) -> Self {
		StateInfo {
			fd,
			state,
			seqno,
			write_buffer: LinkedList::new(),
		}
	}
}

pub struct GuardedData {
	fd_actions: Vec<FdAction>,
	wakeup_fd: i32,
	wakeup_rx: i32,
	wakeup_scheduled: bool,
	handler_events: Vec<HandlerEvent>,
	write_pending: Vec<i32>,
	selector: Option<i32>,
	stop: bool,
}

pub struct EventHandler<F, G, H, K> {
	data: Arc<Mutex<GuardedData>>,
	callbacks: Arc<Mutex<Callbacks<F, G, H, K>>>,
	_pipe_listener: Option<TcpListener>,
	_pipe_stream: Option<TcpStream>,
}

impl<F, G, H, K> EventHandler<F, G, H, K>
where
	F: Fn(&[u8], usize, WriteHandle) -> Result<(), Error> + Send + 'static + Clone + Sync + Unpin,
	G: Fn(u128) -> Result<(), Error> + Send + 'static + Clone + Sync,
	H: Fn(u128) -> Result<(), Error> + Send + 'static + Clone + Sync,
	K: Fn(&[u8], usize, WriteHandle) -> Result<(), Error> + Send + 'static + Clone + Sync + Unpin,
{
	pub fn add_tcp_stream(&mut self, stream: &TcpStream) -> Result<i32, Error> {
		// make sure we have a client on_read handler configured
		{
			let callbacks = self.callbacks.lock().map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
				error
			})?;

			match callbacks.on_read {
				Some(_) => {}
				None => {
					return Err(ErrorKind::SetupError(
						"on_read callback must be registered first".to_string(),
					)
					.into());
				}
			}
		}

		stream.set_nonblocking(true)?;
		#[cfg(any(
			target_os = "linux",
			target_os = "macos",
			dragonfly,
			freebsd,
			netbsd,
			openbsd
		))]
		let ret = self.add_fd(stream.as_raw_fd(), ActionType::AddStream)?;
		#[cfg(target_os = "windows")]
		let ret = self.add_socket(stream.as_raw_socket(), ActionType::AddStream)?;
		Ok(ret)
	}

	pub fn add_tcp_listener(&mut self, listener: &TcpListener) -> Result<i32, Error> {
		// must be nonblocking
		listener.set_nonblocking(true)?;
		#[cfg(any(
			target_os = "linux",
			target_os = "macos",
			dragonfly,
			freebsd,
			netbsd,
			openbsd
		))]
		let ret = self.add_fd(listener.as_raw_fd(), ActionType::AddListener)?;
		#[cfg(target_os = "windows")]
		let ret = self.add_socket(listener.as_raw_socket(), ActionType::AddListener)?;
		Ok(ret)
	}

	#[cfg(target_os = "windows")]
	fn add_socket(&mut self, socket: u64, atype: ActionType) -> Result<i32, Error> {
		let fd = socket.try_into().unwrap_or(0);
		self.add_fd(fd, atype)?;
		Ok(fd)
	}

	fn add_fd(&mut self, fd: i32, atype: ActionType) -> Result<i32, Error> {
		self.ensure_handlers()?;

		let mut data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		if data.selector.is_none() {
			return Err(
				ErrorKind::SetupError("EventHandler must be started first".to_string()).into(),
			);
		}

		let fd_actions = &mut data.fd_actions;
		fd_actions.push(FdAction::new(fd, atype));
		Ok(fd.into())
	}

	fn _remove_fd(&mut self, fd: i32) -> Result<(), Error> {
		let mut data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;
		let fd_actions = &mut data.fd_actions;
		fd_actions.push(FdAction::new(fd, ActionType::Remove));
		Ok(())
	}

	fn ensure_handlers(&self) -> Result<(), Error> {
		let callbacks = self.callbacks.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		match callbacks.on_read {
			Some(_) => {}
			None => {
				return Err(ErrorKind::SetupError(
					"on_read callback must be registered first".to_string(),
				)
				.into());
			}
		}

		match callbacks.on_accept {
			Some(_) => {}
			None => {
				return Err(ErrorKind::SetupError(
					"on_accept callback must be registered first".to_string(),
				)
				.into());
			}
		}

		match callbacks.on_close {
			Some(_) => {}
			None => {
				return Err(ErrorKind::SetupError(
					"on_close callback must be registered first".to_string(),
				)
				.into());
			}
		}

		Ok(())
	}

	fn check_and_set<T>(vec: &mut Vec<T>, i: usize, value: T)
	where
		T: Default,
	{
		let cur_len = vec.len();
		if cur_len <= i {
			for _ in cur_len..i + 1 {
				vec.push(T::default());
			}
		}
		vec[i] = value;
	}

	pub fn set_on_read(&mut self, on_read: F) -> Result<(), Error> {
		let mut callbacks = self.callbacks.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		callbacks.on_read = Some(Box::pin(on_read));

		Ok(())
	}

	pub fn set_on_accept(&mut self, on_accept: G) -> Result<(), Error> {
		let mut callbacks = self.callbacks.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		callbacks.on_accept = Some(Box::pin(on_accept));

		Ok(())
	}

	pub fn set_on_close(&mut self, on_close: H) -> Result<(), Error> {
		let mut callbacks = self.callbacks.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		callbacks.on_close = Some(Box::pin(on_close));

		Ok(())
	}

	pub fn set_on_client_read(&mut self, on_client_read: K) -> Result<(), Error> {
		let mut callbacks = self.callbacks.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		callbacks.on_client_read = Some(Box::pin(on_client_read));

		Ok(())
	}

	pub fn new() -> Self {
		let mut _pipe_stream = None;
		let mut _pipe_listener = None;
		// create the pipe (for wakeups)
		let (rx, tx) = {
			let mut retfds = [0i32; 2];
			let fds: *mut c_int = &mut retfds as *mut _ as *mut c_int;
			#[cfg(target_os = "windows")]
			{
				let res = Self::socket_pipe(fds);
				match res {
					Ok((listener, stream)) => {
						_pipe_stream = Some(stream);
						_pipe_listener = Some(listener);
					}
					Err(e) => {
						log!("Error creating socket_pipe on windows, {}", e.to_string());
					}
				}
			}
			#[cfg(unix)]
			unsafe {
				pipe(fds)
			};
			(retfds[0], retfds[1])
		};

		let callbacks = Callbacks {
			on_read: None,
			on_accept: None,
			on_close: None,
			on_client_read: None,
		};
		let callbacks = Arc::new(Mutex::new(callbacks));

		let guarded_data = GuardedData {
			fd_actions: vec![],
			wakeup_fd: tx,
			wakeup_rx: rx,
			wakeup_scheduled: false,
			handler_events: vec![],
			write_pending: vec![],
			selector: None,
			stop: false,
		};
		let guarded_data = Arc::new(Mutex::new(guarded_data));

		EventHandler {
			data: guarded_data,
			callbacks,
			_pipe_listener,
			_pipe_stream,
		}
	}

	#[cfg(target_os = "linux")]
	pub fn start(&mut self) -> Result<(), Error> {
		// create poll fd
		let selector = epoll_create1(EpollCreateFlags::empty())?;
		self.start_generic(selector)?;
		Ok(())
	}

	#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
	pub fn start(&mut self) -> Result<(), Error> {
		// create the kqueue
		let selector = unsafe { kqueue() };
		self.start_generic(selector)?;
		Ok(())
	}

	#[cfg(target_os = "windows")]
	pub fn start(&mut self) -> Result<(), Error> {
		self.start_generic(0)?;
		Ok(())
	}

	pub fn stop(&self) -> Result<(), Error> {
		let mut guarded_data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		guarded_data.stop = true;
		#[cfg(unix)]
		{
			let buf: *mut c_void = &mut [0u8; 1] as *mut _ as *mut c_void;
			unsafe {
				write(guarded_data.wakeup_fd, buf, 1);
			}
		}
		#[cfg(target_os = "windows")]
		{
			let buf: *mut i8 = &mut [0i8; 1] as *mut _ as *mut i8;
			unsafe {
				ws2_32::send(guarded_data.wakeup_fd.try_into().unwrap_or(0), buf, 1, 0);
			}
		}

		Ok(())
	}

	#[cfg(target_os = "windows")]
	fn socket_pipe(fds: *mut i32) -> Result<(TcpListener, TcpStream), Error> {
		let port = portpicker::pick_unused_port().unwrap_or(9999);
		let listener = TcpListener::bind(format!("127.0.0.1:{}", port))?;
		let stream = TcpStream::connect(format!("127.0.0.1:{}", port))?;
		let res = unsafe {
			accept(
				listener.as_raw_socket().try_into().unwrap_or(0),
				&mut libc::sockaddr {
					..std::mem::zeroed()
				},
				&mut (std::mem::size_of::<libc::sockaddr>() as u32)
					.try_into()
					.unwrap_or(0),
			)
		};
		let fds: &mut [i32] = unsafe { std::slice::from_raw_parts_mut(fds, 2) };
		fds[0] = res as i32;
		fds[1] = stream.as_raw_socket().try_into().unwrap_or(0);
		Ok((listener, stream))
	}

	fn start_generic(&mut self, selector: i32) -> Result<(), Error> {
		{
			let mut guarded_data = self.data.lock().map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
				error
			})?;
			guarded_data.selector = Some(selector);
		}

		let mut fd_locks = vec![];
		let mut on_read_locks = vec![];
		let cloned_guarded_data = self.data.clone();
		let cloned_callbacks = self.callbacks.clone();

		spawn(move || {
			#[cfg(target_os = "windows")]
			let win_selector = unsafe { epoll_create(1) };
			#[cfg(unix)]
			let win_selector = 1 as *mut c_void;
			if win_selector.is_null() {
				log!("win_selector is null. Cannot start");
			} else {
				let res = Self::poll_loop(
					&cloned_guarded_data,
					&cloned_callbacks,
					&mut fd_locks,
					&mut on_read_locks,
					selector,
					win_selector,
				);
				match res {
					Ok(_) => {
						log!("poll loop done");
					}
					Err(e) => {
						log!("FATAL: Unexpected error in poll loop: {}", e.to_string());
					}
				}
			}
		});

		Ok(())
	}

	fn process_handler_events(
		handler_events: Vec<HandlerEvent>,
		write_pending: Vec<i32>,
		evs: &mut Vec<GenericEvent>,
		read_fd_type: &mut Vec<FdType>,
		use_on_client_read: &mut Vec<bool>,
		on_close: Pin<Box<H>>,
		thread_pool: &StaticThreadPool,
		global_lock: &Arc<RwLock<bool>>,
		fd_locks: &mut Vec<Arc<Mutex<StateInfo>>>,
	) -> Result<(), Error> {
		let lock = global_lock.write();
		match lock {
			Ok(_) => {}
			Err(e) => log!("Error obtaining global lock: {}", e),
		}
		for handler_event in &handler_events {
			match handler_event.etype {
				#[cfg(target_os = "windows")]
				HandlerEventType::PauseRead => {
					evs.push(GenericEvent::new(
						handler_event.fd,
						GenericEventType::DelRead,
					));
				}
				#[cfg(target_os = "windows")]
				HandlerEventType::ResumeRead => {
					evs.push(GenericEvent::new(
						handler_event.fd,
						GenericEventType::AddReadET,
					));
				}
				#[cfg(target_os = "windows")]
				HandlerEventType::PauseWrite => {
					evs.push(GenericEvent::new(
						handler_event.fd,
						GenericEventType::DelWrite,
					));
				}
				#[cfg(target_os = "windows")]
				HandlerEventType::ResumeWrite => {
					evs.push(GenericEvent::new(
						handler_event.fd,
						GenericEventType::AddWriteET,
					));
				}
				HandlerEventType::Accept => {
					let fd = handler_event.fd as uintptr_t;
					evs.push(GenericEvent::new(
						handler_event.fd,
						GenericEventType::AddReadET,
					));
					// make sure there's enough space
					let len = read_fd_type.len();
					if fd >= len {
						for _ in len..fd + 1 {
							read_fd_type.push(FdType::Unknown);
						}
					}
					read_fd_type[fd] = FdType::Stream;

					let len = use_on_client_read.len();
					if fd >= len {
						for _ in len..fd + 1 {
							use_on_client_read.push(false);
						}
					}
					use_on_client_read[fd] = false;
				}
				HandlerEventType::Close => {
					let seqno = handler_event.seqno;
					let fd = handler_event.fd;
					Self::do_close(fd, seqno, fd_locks)?;
					match fd_locks[fd as usize].lock() {
						Ok(mut state) => {
							#[cfg(unix)]
							{
								evs.push(GenericEvent::new(state.fd, GenericEventType::DelRead));
								evs.push(GenericEvent::new(state.fd, GenericEventType::DelWrite));
							}
							read_fd_type[handler_event.fd as usize] = FdType::Unknown;
							use_on_client_read[handler_event.fd as usize] = false;
							(*state).write_buffer.clear();
							let on_close = on_close.clone();
							thread_pool.execute(async move {
								match (on_close)(seqno) {
									Ok(_) => {}
									Err(e) => {
										log!("on close handler generated error: {}", e.to_string());
									}
								}
							})?;
						}
						Err(e) => {
							log!(
								"unexpected error getting state lock: {}, fd={}, seqno={}",
								e.to_string(),
								fd,
								seqno,
							);
						}
					}
				}
			}
		}

		// handle write_pending
		for pending in write_pending {
			evs.push(GenericEvent::new(pending, GenericEventType::AddWriteET));
		}
		Ok(())
	}

	#[cfg(target_os = "windows")]
	fn get_events(
		_selector: i32,
		win_selector: *mut c_void,
		input_events: Vec<GenericEvent>,
		output_events: &mut Vec<GenericEvent>,
		filter_set: &mut HashSet<i32>,
	) -> Result<i32, Error> {
		for evt in input_events {
			if evt.etype == GenericEventType::AddReadLT
				|| evt.etype == GenericEventType::AddReadET
				|| evt.etype == GenericEventType::DelWrite
			{
				let op = if filter_set.remove(&evt.fd) {
					EPOLL_CTL_MOD
				} else {
					EPOLL_CTL_ADD
				};
				filter_set.insert(evt.fd);
				let data = epoll_data_t { fd: evt.fd };
				let mut event = epoll_event {
					events: EPOLLIN | EPOLLRDHUP,
					data,
				};
				let res = unsafe {
					epoll_ctl(
						win_selector,
						op.try_into().unwrap_or(0),
						evt.fd as usize,
						&mut event,
					)
				};
				if res != 0 {
					// normal occurance, just means the socket is already closed
					// must remove from filter set for next request
					filter_set.remove(&evt.fd);
				}
			} else if evt.etype == GenericEventType::AddWriteET {
				let op = if filter_set.remove(&evt.fd) {
					EPOLL_CTL_MOD
				} else {
					EPOLL_CTL_ADD
				};
				filter_set.insert(evt.fd);
				let data = epoll_data_t { fd: evt.fd };
				let mut event = epoll_event {
					events: EPOLLIN | EPOLLOUT | EPOLLRDHUP,
					data,
				};
				let res = unsafe {
					epoll_ctl(
						win_selector,
						op.try_into().unwrap_or(0),
						evt.fd.try_into().unwrap_or(0),
						&mut event,
					)
				};
				if res != 0 {
					filter_set.remove(&evt.fd);
					log!(
						"epoll_ctl (write) resulted in an unexpected error: {}, fd={}, op={}, epoll_ctl_add={}",
						errno().to_string(), evt.fd, op, EPOLL_CTL_ADD,
					);
				}
			} else if evt.etype == GenericEventType::DelRead {
				filter_set.remove(&evt.fd);
				let data = epoll_data_t { fd: evt.fd };
				let mut event = epoll_event {
					events: 0, // not used for del
					data,
				};

				let res = unsafe {
					epoll_ctl(
						win_selector,
						EPOLL_CTL_DEL.try_into().unwrap_or(0),
						evt.fd.try_into().unwrap_or(0),
						&mut event,
					)
				};

				if res != 0 {
					log!(
						"epoll_ctl (del) resulted in unexpected error: {}",
						errno().to_string(),
					);
				}
			} else {
				return Err(
					ErrorKind::InternalError(format!("unexpected etype: {:?}", evt.etype)).into(),
				);
			}
		}
		let mut events: [epoll_event; MAX_EVENTS as usize] =
			unsafe { std::mem::MaybeUninit::uninit().assume_init() };
		let results = unsafe { epoll_wait(win_selector, events.as_mut_ptr(), MAX_EVENTS, 1000) };
		let mut ret_count_adjusted = 0;

		if results > 0 {
			for i in 0..results {
				if !(events[i as usize].events & EPOLLOUT == 0) {
					ret_count_adjusted += 1;
					output_events.push(GenericEvent::new(
						unsafe { events[i as usize].data.fd } as i32,
						GenericEventType::AddWriteET,
					));
				}
				if !(events[i as usize].events & EPOLLIN == 0) {
					ret_count_adjusted += 1;
					output_events.push(GenericEvent::new(
						unsafe { events[i as usize].data.fd } as i32,
						GenericEventType::AddReadET,
					));
				}
				if events[i as usize].events & (EPOLLIN | EPOLLOUT) == 0 {
					let fd = unsafe { events[i as usize].data.fd };
					let data = epoll_data_t { fd };
					let mut event = epoll_event {
						events: 0, // not used for del
						data,
					};
					let res = unsafe {
						epoll_ctl(
							win_selector,
							EPOLL_CTL_DEL.try_into().unwrap_or(0),
							fd.try_into().unwrap_or(0),
							&mut event,
						)
					};

					if res != 0 {
						log!(
							"Unexpected error with EPOLLHUP. res = {}, err={}",
							res,
							errno().to_string(),
						);
					}
				}
			}
		}

		Ok(ret_count_adjusted)
	}

	#[cfg(target_os = "linux")]
	fn get_events(
		epollfd: i32,
		_win_selector: *mut c_void,
		input_events: Vec<GenericEvent>,
		output_events: &mut Vec<GenericEvent>,
		filter_set: &mut HashSet<i32>,
	) -> Result<i32, Error> {
		for evt in input_events {
			let mut interest = EpollFlags::empty();

			if evt.etype == GenericEventType::AddReadLT {
				let fd = evt.fd;
				interest |= EpollFlags::EPOLLIN;
				interest |= EpollFlags::EPOLLRDHUP;

				let op = if filter_set.remove(&fd) {
					EpollOp::EpollCtlMod
				} else {
					EpollOp::EpollCtlAdd
				};
				filter_set.insert(fd);

				let mut event = EpollEvent::new(interest, evt.fd as u64);
				let res = epoll_ctl(epollfd, op, evt.fd, &mut event);
				match res {
					Ok(_) => {}
					Err(e) => log!("Error epoll_ctl1: {}, fd={}, op={:?}", e, fd, op),
				}
			} else if evt.etype == GenericEventType::AddReadET {
				let fd = evt.fd;
				interest |= EpollFlags::EPOLLIN;
				interest |= EpollFlags::EPOLLET;
				interest |= EpollFlags::EPOLLRDHUP;

				let op = if filter_set.remove(&fd) {
					EpollOp::EpollCtlMod
				} else {
					EpollOp::EpollCtlAdd
				};
				filter_set.insert(fd);

				let mut event = EpollEvent::new(interest, evt.fd as u64);
				let res = epoll_ctl(epollfd, op, evt.fd, &mut event);
				match res {
					Ok(_) => {}
					Err(e) => log!("Error epoll_ctl2: {}, fd={}, op={:?}", e, fd, op),
				}
			} else if evt.etype == GenericEventType::AddWriteET {
				let fd = evt.fd;
				interest |= EpollFlags::EPOLLOUT;
				interest |= EpollFlags::EPOLLIN;
				interest |= EpollFlags::EPOLLRDHUP;
				interest |= EpollFlags::EPOLLET;

				let op = if filter_set.remove(&fd) {
					EpollOp::EpollCtlMod
				} else {
					EpollOp::EpollCtlAdd
				};
				filter_set.insert(fd);

				let mut event = EpollEvent::new(interest, evt.fd as u64);
				let res = epoll_ctl(epollfd, op, evt.fd, &mut event);
				match res {
					Ok(_) => {}
					Err(e) => log!("Error epoll_ctl3: {}, fd={}, op={:?}", e, fd, op),
				}
			} else if evt.etype == GenericEventType::DelRead {
				interest |= EpollFlags::EPOLLIN;
				filter_set.remove(&evt.fd);
			} else if evt.etype == GenericEventType::DelWrite {
				interest |= EpollFlags::EPOLLOUT;
			} else {
				return Err(
					ErrorKind::InternalError(format!("unexpected etype: {:?}", evt.etype)).into(),
				);
			}
		}

		let empty_event = EpollEvent::new(EpollFlags::empty(), 0);
		let mut events = [empty_event; MAX_EVENTS as usize];
		let results = epoll_wait(epollfd, &mut events, 100);

		let mut ret_count_adjusted = 0;

		match results {
			Ok(results) => {
				if results > 0 {
					for i in 0..results {
						if !(events[i].events() & EpollFlags::EPOLLOUT).is_empty() {
							ret_count_adjusted += 1;
							output_events.push(GenericEvent::new(
								events[i].data() as i32,
								GenericEventType::AddWriteET,
							));
						}
						if !(events[i].events() & EpollFlags::EPOLLIN).is_empty() {
							ret_count_adjusted += 1;
							output_events.push(GenericEvent::new(
								events[i].data() as i32,
								GenericEventType::AddReadET,
							));
						}
					}
				}
			}
			Err(e) => {
				log!("Error with epoll wait = {}", e.to_string());
			}
		}

		Ok(ret_count_adjusted)
	}

	#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
	fn get_events(
		queue: i32,
		_win_selector: *mut c_void,
		input_events: Vec<GenericEvent>,
		output_events: &mut Vec<GenericEvent>,
		_filter_set: &HashSet<i32>,
	) -> Result<i32, Error> {
		let mut kevs = vec![];
		for ev in input_events {
			kevs.push(ev.to_kev());
		}

		let mut ret_kevs = vec![];
		for _ in 0..MAX_EVENTS {
			ret_kevs.push(kevent::new(
				0,
				EventFilter::EVFILT_SYSCOUNT,
				EventFlag::empty(),
				FilterFlag::empty(),
			));
		}

		let ret_count = unsafe {
			kevent(
				queue,
				kevs.as_ptr(),
				kevs.len() as i32,
				ret_kevs.as_mut_ptr(),
				MAX_EVENTS,
				&duration_to_timespec(std::time::Duration::from_millis(100)),
			)
		};

		if ret_count < 0 {
			log!("Error in kevent: kevs={:?}, error={}", kevs, errno());
		}

		let mut ret_count_adjusted = 0;
		for i in 0..ret_count {
			let kev = ret_kevs[i as usize];
			if !kev.flags.contains(EventFlag::EV_DELETE) {
				if kev.filter == EVFILT_WRITE {
					ret_count_adjusted += 1;
					output_events.push(GenericEvent::new(
						kev.ident as i32,
						GenericEventType::AddWriteET,
					));
				}
				if kev.filter == EVFILT_READ {
					ret_count_adjusted += 1;
					output_events.push(GenericEvent::new(
						kev.ident as i32,
						GenericEventType::AddReadET,
					));
				}
			}
		}

		Ok(ret_count_adjusted)
	}

	fn poll_loop(
		guarded_data: &Arc<Mutex<GuardedData>>,
		callbacks: &Arc<Mutex<Callbacks<F, G, H, K>>>,
		fd_locks: &mut Vec<Arc<Mutex<StateInfo>>>,
		on_read_locks: &mut Vec<Arc<Mutex<bool>>>,
		selector: i32,
		win_selector: *mut c_void,
	) -> Result<(), Error> {
		let thread_pool = StaticThreadPool::new()?;
		thread_pool.start(4)?;

		let global_lock = Arc::new(RwLock::new(true));

		let mut seqno = 0u128;

		let rx = {
			let guarded_data = guarded_data.lock().map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
				error
			})?;
			guarded_data.wakeup_rx
		};
		let mut read_fd_type = Vec::new();

		// preallocate some
		read_fd_type.reserve(INITIAL_MAX_FDS);
		for _ in 0..INITIAL_MAX_FDS {
			read_fd_type.push(FdType::Unknown);
		}
		if rx >= INITIAL_MAX_FDS.try_into().unwrap_or(0) {
			for _ in INITIAL_MAX_FDS..(rx + 1) as usize {
				read_fd_type.push(FdType::Unknown);
			}
		}
		read_fd_type[rx as usize] = FdType::Wakeup;

		let mut use_on_client_read = Vec::new();
		// preallocate some
		use_on_client_read.reserve(INITIAL_MAX_FDS);
		for _ in 0..INITIAL_MAX_FDS {
			use_on_client_read.push(false);
		}

		if rx >= INITIAL_MAX_FDS.try_into().unwrap_or(0) {
			for _ in INITIAL_MAX_FDS..(rx + 1) as usize {
				use_on_client_read.push(false);
			}
		}

		// add the wakeup pipe rx here
		let mut output_events = vec![];
		let mut input_events = vec![];
		let mut filter_set = HashSet::new();
		input_events.push(GenericEvent::new(rx, GenericEventType::AddReadLT));
		Self::get_events(
			selector,
			win_selector,
			input_events,
			&mut output_events,
			&mut filter_set,
		)?;

		let mut ret_count;
		loop {
			seqno += 1;
			let to_process;
			let handler_events;
			let write_pending;
			let on_read;
			let on_accept;
			let on_close;
			let on_client_read;
			{
				let mut guarded_data = guarded_data.lock().map_err(|e| {
					let error: Error =
						ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
					error
				})?;
				to_process = guarded_data.fd_actions.clone();
				handler_events = guarded_data.handler_events.clone();
				write_pending = guarded_data.write_pending.clone();
				guarded_data.fd_actions.clear();
				guarded_data.handler_events.clear();
				guarded_data.write_pending.clear();

				// check if a stop is needed
				if guarded_data.stop {
					thread_pool.stop()?;
					#[cfg(unix)]
					{
						let res = unsafe { close(selector) };
						if res != 0 {
							log!("Error closing selector: {}", errno().to_string());
						}
						let res = unsafe { close(guarded_data.wakeup_fd) };
						if res != 0 {
							log!("Error closing selector: {}", errno().to_string());
						}
					}
					#[cfg(target_os = "windows")]
					{
						let res = unsafe { epoll_close(win_selector) };
						if res != 0 {
							log!("Error closing win_selector: {}", errno().to_string());
						}
						let res = unsafe {
							ws2_32::closesocket(guarded_data.wakeup_fd.try_into().unwrap_or(0))
						};
						if res != 0 {
							log!("Error closing selector: {}", errno().to_string());
						}
					}
					break;
				}
			}
			{
				let callbacks = callbacks.lock().map_err(|e| {
					let error: Error =
						ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
					error
				})?;
				on_read = callbacks.on_read.as_ref().unwrap().clone();
				on_accept = callbacks.on_accept.as_ref().unwrap().clone();
				on_close = callbacks.on_close.as_ref().unwrap().clone();
				on_client_read = callbacks.on_client_read.as_ref().unwrap().clone();
			}
			let mut evs: Vec<GenericEvent> = Vec::new();

			for proc in to_process {
				match proc.atype {
					ActionType::AddStream => {
						evs.push(GenericEvent::new(proc.fd, GenericEventType::AddReadET));

						let fd = proc.fd as uintptr_t;
						// make sure there's enough space
						let len = read_fd_type.len();
						if fd >= len {
							for _ in len..fd + 1 {
								read_fd_type.push(FdType::Unknown);
							}
						}

						read_fd_type[fd] = FdType::Stream;

						let len = use_on_client_read.len();
						if fd >= len {
							for _ in len..fd + 1 {
								use_on_client_read.push(false);
							}
						}
						use_on_client_read[fd] = true;
					}
					ActionType::AddListener => {
						evs.push(GenericEvent::new(proc.fd, GenericEventType::AddReadLT));
						let fd = proc.fd as uintptr_t;

						// make sure there's enough space
						let len = read_fd_type.len();
						if fd >= len {
							for _ in len..fd + 1 {
								read_fd_type.push(FdType::Unknown);
							}
						}
						read_fd_type[fd] = FdType::Listener;

						let len = use_on_client_read.len();
						if fd >= len {
							for _ in len..fd + 1 {
								use_on_client_read.push(false);
							}
						}
						use_on_client_read[fd] = false;
					}
					ActionType::Remove => {
						evs.push(GenericEvent::new(proc.fd, GenericEventType::DelRead));
						let fd = proc.fd as uintptr_t;

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
			Self::process_handler_events(
				handler_events,
				write_pending,
				&mut evs,
				&mut read_fd_type,
				&mut use_on_client_read,
				on_close.clone(),
				&thread_pool,
				&global_lock,
				fd_locks,
			)?;
			/*if evs.len() > 0 {
			log!("input events = {:?}", evs);
			}*/
			let mut output_events = vec![];
			ret_count = Self::get_events(
				selector,
				win_selector,
				evs,
				&mut output_events,
				&mut filter_set,
			)?;
			// if no events are returned (on timeout), just bypass following logic and wait
			if ret_count == 0 {
				continue;
			}
			for event in output_events {
				//log!("proc event = {:?}", event);
				if event.etype == GenericEventType::AddWriteET {
					let res = Self::process_event_write(
						event.fd as i32,
						&thread_pool,
						guarded_data,
						&global_lock,
						fd_locks,
					);
					match res {
						Ok(_) => {}
						Err(e) => {
							log!("Unexpected error in poll loop: {}", e.to_string());
						}
					}
				}
				if event.etype == GenericEventType::AddReadET
					|| event.etype == GenericEventType::AddReadLT
				{
					let res = Self::process_event_read(
						event.fd as i32,
						&mut read_fd_type,
						&thread_pool,
						guarded_data,
						on_read.clone(),
						on_accept.clone(),
						on_client_read.clone(),
						use_on_client_read[event.fd as usize],
						fd_locks,
						on_read_locks,
						seqno,
						&global_lock,
					);

					match res {
						Ok(_) => {}
						Err(e) => {
							log!("Unexpected error in poll loop: {}", e.to_string());
						}
					}
				}
			}
		}

		Ok(())
	}

	fn write_loop(fd: i32, statefd: i32, write_buffer: &mut WriteBuffer) -> Result<u16, Error> {
		let initial_len = write_buffer.len;
		loop {
			if statefd != fd {
				return Err(ErrorKind::StaleFdError(format!(
					"write to closed fd: {}, statefd = {}",
					fd, statefd,
				))
				.into());
			}
			#[cfg(unix)]
			let len = {
				let buf: *mut c_void = &mut write_buffer.buffer
					[(write_buffer.offset as usize)..(write_buffer.len as usize)]
					as *mut _ as *mut c_void;

				unsafe { write(fd, buf, (write_buffer.len - write_buffer.offset).into()) }
			};
			#[cfg(target_os = "windows")]
			let len = {
				let buf: *mut i8 = &mut write_buffer.buffer
					[(write_buffer.offset as usize)..(write_buffer.len as usize)]
					as *mut _ as *mut i8;

				unsafe {
					ws2_32::send(
						fd.try_into().unwrap_or(0),
						buf,
						(write_buffer.len - write_buffer.offset).into(),
						0,
					)
				}
			};
			if len >= 0 {
				if len
					== (write_buffer.len as isize - write_buffer.offset as isize)
						.try_into()
						.unwrap_or(0)
				{
					// we're done
					write_buffer.offset += len as u16;
					write_buffer.len -= len as u16;
					return Ok(initial_len);
				} else {
					// update values and write again
					write_buffer.offset += len as u16;
					write_buffer.len -= len as u16;
				}
			} else {
				if errno().0 == EAGAIN {
					// break because we're edge triggered.
					// a new event occurs.

					return Ok(initial_len - write_buffer.len);
				} else {
					// this is an actual write error.
					// close the connection.
					return Err(
						ErrorKind::ConnectionCloseError(format!("connection closed",)).into(),
					);
				}
			}
		}
	}

	fn do_close(
		fd: i32,
		seqno: u128,
		fd_locks: &mut Vec<Arc<Mutex<StateInfo>>>,
	) -> Result<(), Error> {
		let state = fd_locks[fd as usize].lock();
		match state {
			Ok(mut state) => {
				if state.fd == fd {
					if state.state == State::Closing {
						#[cfg(unix)]
						let res = unsafe { close(state.fd) };
						#[cfg(target_os = "windows")]
						let res = unsafe { ws2_32::closesocket(state.fd.try_into().unwrap_or(0)) };
						if res == 0 {
							state.state = State::Closed;
						} else {
							let e = errno();
							log!("error closing socket: {}", e.to_string());
							return Err(
								ErrorKind::InternalError("Already closed".to_string()).into()
							);
						}
					} else {
						return Err(ErrorKind::InternalError("Already closed".to_string()).into());
					}
				} else {
					return Err(ErrorKind::InternalError("FD mismatch".to_string()).into());
				}
			}
			Err(e) => {
				log!(
					"unexpected error obtaining fd_lock to close: {}, fd={}, seqno={}",
					e.to_string(),
					fd,
					seqno
				);
				return Err(ErrorKind::InternalError("can't obtain lock".to_string()).into());
			}
		}
		Ok(())
	}

	fn write_until_block(
		fd: i32,
		state_info: &mut StateInfo,
		guarded_data: &Arc<Mutex<GuardedData>>,
	) -> Result<bool, Error> {
		let mut complete = true;
		loop {
			let (ret, total_len, front_close, front_seqno) = {
				let front = state_info.write_buffer.front_mut();
				if front.is_none() {
					break;
				}
				let front = front.unwrap();
				let total_len = front.len;
				(
					Self::write_loop(fd, state_info.fd, front),
					total_len,
					front.close,
					front.connection_seqno,
				)
			};

			match ret {
				Ok(len) => {
					if len == total_len {
						if front_close {
							Self::push_handler_event_with_fd_lock(
								fd,
								HandlerEventType::Close,
								guarded_data,
								state_info,
								false,
								front_seqno,
							)?;
						}
						state_info.write_buffer.pop_front();
					} else {
						// we didn't complete, we need to break
						// we had to block so a new
						// edge triggered event will occur
						complete = false;
						break;
					}
				}
				Err(e) => {
					log!("write error: {}", e);
					Self::push_handler_event_with_fd_lock(
						fd,
						HandlerEventType::Close,
						guarded_data,
						state_info,
						false,
						front_seqno,
					)?;
					break;
				}
			}
		}

		Ok(complete)
	}

	fn process_event_write(
		fd: i32,
		thread_pool: &StaticThreadPool,
		guarded_data: &Arc<Mutex<GuardedData>>,
		global_lock: &Arc<RwLock<bool>>,
		fd_locks: &mut Vec<Arc<Mutex<StateInfo>>>,
	) -> Result<(), Error> {
		#[cfg(target_os = "windows")]
		{
			// if windows push pause event before proceeding
			let guarded_data = guarded_data.lock();
			match guarded_data {
				Ok(mut guarded_data) => {
					let nevent = HandlerEvent::new(fd, HandlerEventType::PauseWrite, 0);
					guarded_data.handler_events.push(nevent);
				}
				Err(e) => {
					log!(
						"unexpected error getting guareded_data lock: {}",
						e.to_string()
					);
				}
			}
		}

		let state_info = fd_locks[fd as usize].clone();
		let guarded_data = guarded_data.clone();
		let global_lock = global_lock.clone();
		#[cfg(target_os = "windows")]
		let mut fd_locks = fd_locks.clone();
		thread_pool
			.execute(async move {
				#[cfg(target_os = "windows")]
				let mut seqno = 0;
				#[cfg(target_os = "windows")]
				let mut complete = true;
				{
					let lock = global_lock.read();
					match lock {
						Ok(_) => {}
						Err(e) => log!("Unexpected error obtaining write lock: {}", e),
					}
					{
						let state_info = state_info.lock();
						match state_info {
							Ok(mut state_info) => {
								#[cfg(target_os = "windows")]
								{
									seqno = state_info.seqno;
								}
								let res =
									Self::write_until_block(fd, &mut state_info, &guarded_data);
								#[cfg(target_os = "windows")]
								match res {
									Ok(c) => {
										complete = c;
									}
									Err(e) => {
										log!(
											"unexpected error in process_event_write: {}",
											e.to_string()
										);
									}
								}
								#[cfg(unix)]
								match res {
									Ok(_) => {}
									Err(e) => {
										log!(
											"unexpected error in process_event_write: {}",
											e.to_string()
										);
									}
								}
							}
							Err(e) => {
								log!(
									"unexpected error with locking write_buffer: {}",
									e.to_string()
								);
							}
						}
					}
				}

				#[cfg(target_os = "windows")]
				{
					if !complete {
						let res = Self::push_handler_event(
							fd,
							HandlerEventType::ResumeWrite,
							&guarded_data,
							&mut fd_locks,
							false,
							seqno,
						);
						match res {
							Ok(_) => {}
							Err(e) => {
								log!("Error pushing handler event: {}", e);
							}
						}
					}
				}
			})
			.map_err(|e| {
				let error: Error =
					ErrorKind::InternalError(format!("write thread pool error: {}", e)).into();
				error
			})?;

		Ok(())
	}

	fn process_event_read(
		fd: i32,
		read_fd_type: &mut Vec<FdType>,
		thread_pool: &StaticThreadPool,
		guarded_data: &Arc<Mutex<GuardedData>>,
		on_read: Pin<Box<F>>,
		on_accept: Pin<Box<G>>,
		on_client_read: Pin<Box<K>>,
		use_on_client_read: bool,
		fd_locks: &mut Vec<Arc<Mutex<StateInfo>>>,
		on_read_locks: &mut Vec<Arc<Mutex<bool>>>,
		seqno: u128,
		global_lock: &Arc<RwLock<bool>>,
	) -> Result<(), Error> {
		let fd_type = &read_fd_type[fd as usize];
		match fd_type {
			FdType::Listener => {
				let lock = global_lock.write();
				match lock {
					Ok(_) => {}
					Err(e) => log!("Unexpected error obtaining read lock, {}", e),
				}
				#[cfg(unix)]
				let res = unsafe {
					accept(
						fd.try_into().unwrap_or(0),
						&mut libc::sockaddr {
							..std::mem::zeroed()
						},
						&mut (std::mem::size_of::<libc::sockaddr>() as u32)
							.try_into()
							.unwrap_or(0),
					)
				};

				#[cfg(target_os = "windows")]
				let res = unsafe {
					ws2_32::accept(
						fd.try_into().unwrap_or(0),
						&mut winapi::ws2def::SOCKADDR {
							..std::mem::zeroed()
						},
						&mut (std::mem::size_of::<winapi::ws2def::SOCKADDR>() as u32)
							.try_into()
							.unwrap_or(0),
					)
				};
				if res > 0 {
					let len = read_fd_type.len();
					if res as usize >= len {
						for _ in len..res as usize + 1 {
							read_fd_type.push(FdType::Unknown);
						}
					}

					if on_read_locks.len() <= res as usize {
						Self::check_and_set(
							on_read_locks,
							res as usize,
							Arc::new(Mutex::new(false)),
						);
					}

					if fd_locks.len() <= res as usize {
						Self::check_and_set(
							fd_locks,
							res as usize,
							Arc::new(Mutex::new(StateInfo::new(
								res.try_into().unwrap_or(0),
								State::Normal,
								seqno,
							))),
						);
					}

					{
						let current_seqno = fd_locks[res as usize].lock();
						match current_seqno {
							Ok(mut current_seqno) => {
								*current_seqno = StateInfo::new(
									res.try_into().unwrap_or(0),
									State::Normal,
									seqno,
								);
							}
							Err(e) => {
								log!("Error getting seqno: {}", e.to_string());
								return Err(ErrorKind::InternalError(
									"unexpected error obtaining seqno".to_string(),
								)
								.into());
							}
						}
					}

					match fd_locks[res as usize].lock() {
						Ok(mut state) => {
							state.fd = res.try_into().unwrap_or(0);
							state.state = State::Normal;
							state.seqno = seqno;
						}
						Err(e) => {
							log!(
								"unexpected error obtaining fd_lock: {}, fd={}, seqno={}",
								e.to_string(),
								fd,
								seqno,
							);
						}
					}

					// set non-blocking
					#[cfg(unix)]
					{
						let fcntl_res = unsafe { fcntl(res, libc::F_SETFL, libc::O_NONBLOCK) };
						if fcntl_res < 0 {
							let e = errno().to_string();
							return Err(
								ErrorKind::InternalError(format!("fcntl error: {}", e)).into()
							);
						}
					}
					#[cfg(target_os = "windows")]
					{
						let fionbio = 0x8004667eu32;
						let ioctl_res = unsafe {
							ws2_32::ioctlsocket(
								res.try_into().unwrap_or(0),
								fionbio as c_int,
								&mut 1,
							)
						};

						if ioctl_res != 0 {
							log!("complete fion with error: {}", errno().to_string());
						}
					}
					let guarded_data = guarded_data.clone();

					let accept_res = Self::process_accept_result(
						fd,
						res.try_into().unwrap_or(0),
						&guarded_data,
						fd_locks,
					);
					match accept_res {
						Ok(_) => {}
						Err(e) => {
							log!("process_accept_result resulted in: {}", e.to_string())
						}
					}
					let accept_res = (on_accept)(seqno as u128);
					match accept_res {
						Ok(_) => {}
						Err(e) => log!("on_accept callback resulted in: {}", e.to_string()),
					}
					Ok(())
				} else {
					Self::process_accept_err(fd, "accept error".to_string())
				}?;
			}
			FdType::Stream => {
				#[cfg(target_os = "windows")]
				{
					// if windows push pause event before proceeding
					let guarded_data = guarded_data.lock();
					match guarded_data {
						Ok(mut guarded_data) => {
							let nevent = HandlerEvent::new(fd, HandlerEventType::PauseRead, seqno);
							guarded_data.handler_events.push(nevent);
						}
						Err(e) => {
							log!(
								"unexpected error getting guareded_data lock: {}",
								e.to_string()
							);
						}
					}
				}

				let guarded_data = guarded_data.clone();
				let fd_lock = fd_locks[fd as usize].clone();
				let mut fd_locks = fd_locks.clone();
				let on_read_locks = on_read_locks.clone();
				let global_lock = global_lock.clone();
				thread_pool
					.execute(async move {
						let lock = global_lock.read();
						match lock {
							Ok(_) => {}
							Err(e) => log!("Unexpected error obtaining read lock: {}", e),
						}
						let mut buf = [0u8; BUFFER_SIZE];
						loop {
							let on_read_lock = on_read_locks[fd as usize].lock();
							match on_read_lock {
								Ok(_) => {}
								Err(e) => {
									log!("unexpected error obtaining on_read_lock: {}", e);
								}
							}
							let (seqno, len) = {
								let fd_lock = fd_lock.lock();
								let seqno = match fd_lock {
									Ok(ref state_info) => (*state_info).seqno,
									Err(e) => {
										log!(
											"Unexpected Error obtaining read lock: {}",
											e.to_string()
										);
										break;
									}
								};
								#[cfg(unix)]
								let len = {
									let cbuf: *mut c_void = &mut buf as *mut _ as *mut c_void;
									unsafe { read(fd, cbuf, BUFFER_SIZE) }
								};
								#[cfg(target_os = "windows")]
								let len = {
									let cbuf: *mut i8 = &mut buf as *mut _ as *mut i8;
									unsafe {
										ws2_32::recv(
											fd.try_into().unwrap_or(0),
											cbuf,
											BUFFER_SIZE.try_into().unwrap_or(0),
											0,
										)
									}
								};

								(seqno, len)
							};

							if len >= 0 {
								let _ = Self::process_read_result(
									fd,
									len as usize,
									buf,
									&guarded_data,
									&mut fd_locks,
									on_read.clone(),
									on_client_read.clone(),
									use_on_client_read,
									seqno,
								);
								if len == 0 {
									break;
								}

								// break on windows
								#[cfg(target_os = "windows")]
								break;
							} else {
								let e = errno();
								if e.0 != EAGAIN {
									let _ = Self::process_read_err(
										fd,
										e.to_string(),
										&guarded_data,
										&mut fd_locks,
										seqno,
									);
								}
								break;
							};
						}
						// resume read here - windows
						#[cfg(target_os = "windows")]
						{
							let res = Self::push_handler_event(
								fd,
								HandlerEventType::ResumeRead,
								&guarded_data,
								&mut fd_locks,
								false,
								seqno,
							);
							match res {
								Ok(_) => {}
								Err(e) => {
									log!("Error pushing handler event: {}", e);
								}
							}
						}
					})
					.map_err(|e| {
						let error: Error =
							ErrorKind::InternalError(format!("read thread pool error: {}", e))
								.into();
						error
					})?;
			}
			FdType::Unknown => {
				log!("unexpected fd_type (unknown) for fd: {}", fd);
			}
			FdType::Wakeup => {
				#[cfg(unix)]
				{
					let cbuf: *mut c_void = &mut [0u8; 1] as *mut _ as *mut c_void;
					unsafe { read(fd, cbuf, 1) }
				};
				#[cfg(target_os = "windows")]
				{
					let cbuf: *mut i8 = &mut [0u8; 1] as *mut _ as *mut i8;
					unsafe {
						ws2_32::recv(
							fd.try_into().unwrap_or(0),
							cbuf,
							BUFFER_SIZE.try_into().unwrap_or(0),
							0,
						)
					}
				};

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
		fd: i32,
		len: usize,
		buf: [u8; BUFFER_SIZE],
		guarded_data: &Arc<Mutex<GuardedData>>,
		fd_locks: &mut Vec<Arc<Mutex<StateInfo>>>,
		on_read: Pin<Box<F>>,
		on_client_read: Pin<Box<K>>,
		use_on_client_read: bool,
		connection_seqno: u128,
	) -> Result<(), Error> {
		if len > 0 {
			// build write handle
			let wh = WriteHandle::new(
				fd,
				guarded_data.clone(),
				connection_seqno,
				fd_locks[fd as usize].clone(),
			);

			let result = match use_on_client_read {
				true => (on_client_read)(&buf, len, wh),
				false => (on_read)(&buf, len, wh),
			};

			match result {
				Ok(_) => {}
				Err(e) => {
					log!("Client callback resulted in error: {}", e.to_string());
				}
			}
		} else {
			// close
			Self::push_handler_event(
				fd,
				HandlerEventType::Close,
				guarded_data,
				fd_locks,
				false,
				connection_seqno,
			)?;
		}
		Ok(())
	}

	fn process_read_err(
		fd: i32,
		_error: String,
		guarded_data: &Arc<Mutex<GuardedData>>,
		fd_locks: &mut Vec<Arc<Mutex<StateInfo>>>,
		connection_seqno: u128,
	) -> Result<(), Error> {
		Self::push_handler_event(
			fd,
			HandlerEventType::Close,
			guarded_data,
			fd_locks,
			false,
			connection_seqno,
		)?;
		Ok(())
	}

	fn process_accept_result(
		_acceptor: i32,
		nfd: i32,
		guarded_data: &Arc<Mutex<GuardedData>>,
		fd_locks: &mut Vec<Arc<Mutex<StateInfo>>>,
	) -> Result<(), Error> {
		Self::push_handler_event(
			nfd,
			HandlerEventType::Accept,
			guarded_data,
			fd_locks,
			false,
			0,
		)
		.map_err(|e| {
			let error: Error =
				ErrorKind::InternalError(format!("push handler event error: {}", e.to_string()))
					.into();
			error
		})?;
		Ok(())
	}

	fn process_accept_err(_acceptor: i32, error: String) -> Result<(), Error> {
		log!("error on acceptor: {}", error);
		Ok(())
	}

	fn push_handler_event_with_fd_lock(
		fd: i32,
		event_type: HandlerEventType,
		guarded_data: &Arc<Mutex<GuardedData>>,
		state: &mut StateInfo,
		wakeup: bool,
		seqno: u128,
	) -> Result<(), Error> {
		if event_type == HandlerEventType::Close {
			if state.state == State::Normal {
				state.state = State::Closing;
			} else {
				return Ok(()); // nothing more to do
			}
		}

		Self::generic_handler_complete(fd, guarded_data, event_type, seqno, wakeup)?;
		Ok(())
	}

	fn push_handler_event(
		fd: i32,
		event_type: HandlerEventType,
		guarded_data: &Arc<Mutex<GuardedData>>,
		fd_locks: &mut Vec<Arc<Mutex<StateInfo>>>,
		wakeup: bool,
		seqno: u128,
	) -> Result<(), Error> {
		if event_type == HandlerEventType::Close {
			match fd_locks[fd as usize].lock() {
				Ok(mut state) => {
					if state.fd == fd {
						if state.state == State::Normal {
							state.state = State::Closing;
						} else {
							return Ok(()); // return nothing more to do
						}
					} else {
						return Ok(()); // return nothing more to do
					}
				}
				Err(e) => {
					log!(
                        "unexpected error obtaining lock for fd_lock push_handler_event, e={},fd={},event_type={:?},seqno={}",
                        e.to_string(),
                        fd,
                        event_type,
                        seqno,
                    );
					return Ok(()); // we continue with this error
				}
			}
		}
		Self::generic_handler_complete(fd, guarded_data, event_type, seqno, wakeup)?;
		Ok(())
	}

	fn generic_handler_complete(
		fd: i32,
		guarded_data: &Arc<Mutex<GuardedData>>,
		event_type: HandlerEventType,
		seqno: u128,
		wakeup: bool,
	) -> Result<(), Error> {
		{
			let guarded_data = guarded_data.lock();
			let mut wakeup_fd = 0;
			let mut wakeup_scheduled = false;
			match guarded_data {
				Ok(mut guarded_data) => {
					let nevent = HandlerEvent::new(fd, event_type.clone(), seqno);
					guarded_data.handler_events.push(nevent);

					if wakeup {
						wakeup_scheduled = guarded_data.wakeup_scheduled;
						if !wakeup_scheduled {
							guarded_data.wakeup_scheduled = true;
						}
						wakeup_fd = guarded_data.wakeup_fd;
					}
				}
				Err(e) => {
					log!("Unexpected handler error: {}", e.to_string());
				}
			}
			if wakeup && !wakeup_scheduled {
				#[cfg(unix)]
				{
					let buf: *mut c_void = &mut [0u8; 1] as *mut _ as *mut c_void;
					unsafe {
						write(wakeup_fd, buf, 1);
					}
				}
				#[cfg(target_os = "windows")]
				{
					let buf: *mut i8 = &mut [0i8; 1] as *mut _ as *mut i8;
					unsafe {
						ws2_32::send(wakeup_fd.try_into().unwrap_or(0), buf, 1, 0);
					}
				}
			}
		}
		Ok(())
	}
}

#[test]
fn test_echo() -> Result<(), Error> {
	use std::io::Write;
	use std::net::TcpListener;
	use std::net::TcpStream;

	let x = Arc::new(Mutex::new(0));
	let x_clone = x.clone();

	let listener = TcpListener::bind("127.0.0.1:9981")?;
	let mut stream = TcpStream::connect("127.0.0.1:9981")?;
	let _stream2 = TcpStream::connect("127.0.0.1:9981")?;
	let mut eh = EventHandler::new();

	// echo
	eh.set_on_read(|buf, len, wh| {
		let _ = wh.write(buf, 0, len, false);
		Ok(())
	})?;

	eh.set_on_accept(|_| Ok(()))?;
	eh.set_on_close(|_| Ok(()))?;
	eh.set_on_client_read(move |buf, len, _wh| {
		assert_eq!(len, 5);
		assert_eq!(buf[0], 1);
		assert_eq!(buf[1], 2);
		assert_eq!(buf[2], 3);
		assert_eq!(buf[3], 4);
		assert_eq!(buf[4], 5);
		let mut x = x.lock().unwrap();
		(*x) += 5;
		Ok(())
	})?;

	eh.start()?;

	eh.add_tcp_listener(&listener)?;
	eh.add_tcp_stream(&stream)?;
	std::thread::sleep(std::time::Duration::from_millis(1000));
	stream.write(&[1, 2, 3, 4, 5])?;
	// wait long enough to make sure the client got the message
	std::thread::sleep(std::time::Duration::from_millis(100));
	let x = x_clone.lock().unwrap();
	assert_eq!((*x), 5);
	Ok(())
}

#[test]
fn test_close() -> Result<(), Error> {
	use std::io::Read;
	use std::io::Write;
	use std::net::TcpListener;
	use std::net::TcpStream;

	let listener = TcpListener::bind("127.0.0.1:9982")?;
	let mut stream = TcpStream::connect("127.0.0.1:9982")?;
	let mut eh = EventHandler::new();

	// echo
	eh.set_on_read(|buf, len, wh| {
		match len {
			// just close the connection with no response
			7 => {
				let _ = wh.close();
			}
			// close if len == 5, otherwise keep open
			_ => {
				let _ = wh.write(buf, 0, len, len == 5);
			}
		}
		Ok(())
	})?;

	eh.set_on_accept(|_| Ok(()))?;
	eh.set_on_close(|_| Ok(()))?;
	eh.set_on_client_read(move |_buf, _len, _wh| Ok(()))?;

	eh.start()?;
	eh.add_tcp_listener(&listener)?;

	stream.write(&[1, 2, 3, 4, 5, 6])?;
	let mut buf = [0u8; 1000];
	let len = stream.read(&mut buf)?;
	assert_eq!(len, 6);
	stream.write(&[1, 2, 3, 4, 5])?;
	let len = stream.read(&mut buf)?;
	assert_eq!(len, 5);
	let len = stream.read(&mut buf)?;
	assert_eq!(len, 0); // means connection closed

	let mut stream2 = TcpStream::connect("127.0.0.1:9982")?;
	stream2.write(&[1, 2, 3, 4, 5, 6, 7])?;
	let len = stream2.read(&mut buf)?;
	assert_eq!(len, 0); // means connection closed

	Ok(())
}

#[test]
fn test_client() -> Result<(), Error> {
	use std::io::Write;
	use std::net::TcpListener;
	use std::net::TcpStream;

	let listener = TcpListener::bind("127.0.0.1:9983")?;
	let mut stream = TcpStream::connect("127.0.0.1:9983")?;
	let mut eh = EventHandler::new();

	// echo
	eh.set_on_read(|buf, len, wh| {
		match len {
			// just close the connection with no response
			7 => {
				let _ = wh.close();
			}
			// close if len == 5, otherwise keep open
			_ => {
				let _ = wh.write(buf, 0, len, len == 5);
			}
		}
		Ok(())
	})?;

	eh.set_on_accept(|_| Ok(()))?;
	eh.set_on_close(|_| Ok(()))?;
	eh.set_on_client_read(move |buf, len, _wh| {
		log!("client_read={:?}", &buf[0..len]);
		assert_eq!(&buf[0..len], [1, 2, 3, 4, 5, 6]);
		Ok(())
	})?;

	eh.start()?;
	eh.add_tcp_listener(&listener)?;
	eh.add_tcp_stream(&stream)?;
	std::thread::sleep(std::time::Duration::from_millis(1000));
	stream.write(&[1, 2, 3, 4, 5, 6])?;
	std::thread::sleep(std::time::Duration::from_millis(1000));
	Ok(())
}

#[test]
fn test_stop() -> Result<(), Error> {
	use std::io::Write;
	use std::net::TcpListener;
	use std::net::TcpStream;

	let listener = TcpListener::bind("127.0.0.1:9984")?;
	let mut stream = TcpStream::connect("127.0.0.1:9984")?;
	let mut eh = EventHandler::new();
	let x = Arc::new(Mutex::new(0));
	let xclone = x.clone();

	// echo
	eh.set_on_read(move |buf, len, wh| {
		let mut x = xclone.lock().unwrap();
		*x += 1;
		match len {
			// just close the connection with no response
			7 => {
				let _ = wh.close();
			}
			// close if len == 5, otherwise keep open
			_ => {
				let _ = wh.write(buf, 0, len, len == 5);
			}
		}
		Ok(())
	})?;

	eh.set_on_accept(|_| Ok(()))?;
	eh.set_on_close(|_| Ok(()))?;
	eh.set_on_client_read(move |buf, len, _wh| {
		assert_eq!(&buf[0..len], [1, 2, 3, 4, 5, 6]);
		Ok(())
	})?;

	eh.start()?;
	eh.add_tcp_listener(&listener)?;
	eh.add_tcp_stream(&stream)?;
	std::thread::sleep(std::time::Duration::from_millis(1000));
	stream.write(&[1, 2, 3, 4, 5, 6])?;
	std::thread::sleep(std::time::Duration::from_millis(1000));
	eh.stop()?;
	std::thread::sleep(std::time::Duration::from_millis(1000));
	stream.write(&[1, 2, 3, 4, 5, 6, 7])?;
	std::thread::sleep(std::time::Duration::from_millis(1000));
	let x = x.lock().unwrap();
	assert_eq!(*x, 1);
	Ok(())
}

#[test]
fn test_large_messages() -> Result<(), Error> {
	use std::io::Write;
	use std::net::TcpListener;
	use std::net::TcpStream;

	let buf_len = 100_000_000;
	let listener = TcpListener::bind("127.0.0.1:9944")?;
	let mut stream = TcpStream::connect("127.0.0.1:9944")?;
	let mut eh = EventHandler::new();
	let x = Arc::new(Mutex::new(0));
	let xclone = x.clone();

	let data_buf = Arc::new(Mutex::new(vec![]));
	eh.set_on_read(move |buf, len, _wh| {
		let mut data_buf = data_buf.lock().unwrap();
		for i in 0..len {
			data_buf.push(buf[i]);
			if buf[i] == 128 {
				// complete
				assert_eq!(data_buf.len(), buf_len + 1);
				for i in 0..buf_len {
					if data_buf[i] != (i % 123) as u8 {
						log!("i={}", i);
					}
					assert_eq!(data_buf[i], (i % 123) as u8);
				}
			}
		}
		let mut x = xclone.lock().unwrap();
		*x = data_buf.len();

		Ok(())
	})?;

	eh.set_on_accept(|_| Ok(()))?;
	eh.set_on_close(|_| Ok(()))?;
	eh.set_on_client_read(move |_buf, _len, _wh| Ok(()))?;

	eh.start()?;
	eh.add_tcp_listener(&listener)?;
	std::thread::sleep(std::time::Duration::from_millis(1000));
	let mut msg = vec![];
	for i in 0..buf_len {
		msg.push((i % 123) as u8);
	}
	msg.push(128 as u8);
	stream.write(&msg)?;
	std::thread::sleep(std::time::Duration::from_millis(10000));
	for i in 0..buf_len {
		if msg[i] != (i % 123) as u8 {
			log!("i={}", i);
		}
		assert_eq!(msg[i], (i % 123) as u8);
	}
	assert_eq!(*(x.lock().unwrap()), buf_len + 1);
	Ok(())
}
