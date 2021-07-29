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
use crate::util::threadpool::StaticThreadPool;
use crate::util::{Error, ErrorKind};
use libc::uintptr_t;
use log::*;
use nioruntime_libnio::ActionType;
use nix::errno::Errno;
use nix::fcntl::fcntl;
use nix::fcntl::OFlag;
use nix::fcntl::F_SETFL;
#[cfg(target_os = "linux")]
use nix::sys::epoll::{epoll_create1, EpollCreateFlags};
use nix::sys::socket::accept;
use nix::unistd::close;
use nix::unistd::pipe;
use nix::unistd::read;
use nix::unistd::write;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::net::TcpListener;
use std::net::TcpStream;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::spawn;

const INITIAL_MAX_FDS: usize = 100;
const BUFFER_SIZE: usize = 1024;
#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
const MAX_EVENTS_KQUEUE: i32 = 100;

#[derive(PartialEq)]
enum GenericEventType {
	AddReadET,
	AddReadLT,
	DelRead,
	AddWriteET,
	DelWrite,
}

struct GenericEvent {
	fd: RawFd,
	etype: GenericEventType,
}

impl GenericEvent {
	fn new(fd: RawFd, etype: GenericEventType) -> Self {
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
}

impl WriteHandle {
	pub fn new(fd: i32, guarded_data: Arc<Mutex<GuardedData>>, connection_id: u128) -> Self {
		WriteHandle {
			fd,
			guarded_data,
			connection_id,
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

		// update GuardedData with our write_buffers, notification message, and wakeup
		let id_idx = self.fd as uintptr_t;
		let (fd, wakeup_scheduled) = {
			let mut guarded_data = self.guarded_data.lock().map_err(|e| {
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
					close: match rem <= BUFFER_SIZE {
						true => close,
						false => false,
					},
					connection_seqno: self.connection_id,
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

			guarded_data.write_pending.push(self.fd.into());
			Self::do_wakeup_with_lock(&mut *guarded_data)?
		};

		if !wakeup_scheduled {
			write(fd, &[0u8; 1])?;
		}

		Ok(())
	}

	fn do_wakeup_with_lock(data: &mut GuardedData) -> Result<(RawFd, bool), Error> {
		let wakeup_scheduled = data.wakeup_scheduled;
		if !wakeup_scheduled {
			data.wakeup_scheduled = true;
		}
		Ok((data.wakeup_fd, wakeup_scheduled))
	}
}

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

#[derive(Clone, PartialEq, Debug)]
enum HandlerEventType {
	Accept,
	Close,
}

#[derive(Clone, PartialEq, Debug)]
struct HandlerEvent {
	etype: HandlerEventType,
	fd: RawFd,
	seqno: u128,
}

impl HandlerEvent {
	fn new(fd: RawFd, etype: HandlerEventType, seqno: u128) -> Self {
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

struct WriteBuffer {
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

pub struct GuardedData {
	fd_actions: Vec<RawFdAction>,
	wakeup_fd: RawFd,
	wakeup_rx: RawFd,
	wakeup_scheduled: bool,
	handler_events: Vec<HandlerEvent>,
	write_pending: Vec<RawFd>,
	write_buffers: Vec<LinkedList<WriteBuffer>>,
	selector: Option<RawFd>,
	seqno_map: HashMap<u128, RawFd>,
}

pub struct EventHandler<F, G, H, K> {
	data: Arc<Mutex<GuardedData>>,
	callbacks: Arc<Mutex<Callbacks<F, G, H, K>>>,
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

	fn add_fd(&mut self, fd: RawFd, atype: ActionType) -> Result<i32, Error> {
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
		fd_actions.push(RawFdAction::new(fd, atype));
		Ok(fd.into())
	}

	fn _remove_fd(&mut self, fd: RawFd) -> Result<(), Error> {
		let mut data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;
		let fd_actions = &mut data.fd_actions;
		fd_actions.push(RawFdAction::new(fd, ActionType::Remove));
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

	fn do_wakeup_with_lock(data: &mut GuardedData) -> Result<(RawFd, bool), Error> {
		let wakeup_scheduled = data.wakeup_scheduled;
		if !wakeup_scheduled {
			data.wakeup_scheduled = true;
		}
		Ok((data.wakeup_fd, wakeup_scheduled))
	}

	fn do_wakeup(data: &Arc<Mutex<GuardedData>>) -> Result<(), Error> {
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

	pub fn wakeup(&mut self) -> Result<(), Error> {
		Self::do_wakeup(&self.data)?;
		Ok(())
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
		// create the pipe (for wakeups)
		let (rx, tx) = pipe().unwrap();

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
			write_buffers: vec![LinkedList::new()],
			selector: None,
			seqno_map: HashMap::new(),
		};
		let guarded_data = Arc::new(Mutex::new(guarded_data));

		EventHandler {
			data: guarded_data,
			callbacks,
		}
	}

	#[cfg(target_os = "linux")]
	pub fn start(&self) -> Result<(), Error> {
		// create poll fd
		let selector = epoll_create1(EpollCreateFlags::empty())?;
		self.start_generic(selector)?;
		Ok(())
	}

	#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
	pub fn start(&self) -> Result<(), Error> {
		// create the kqueue
		let selector = unsafe { kqueue() };
		self.start_generic(selector)?;
		Ok(())
	}

	pub fn start_generic(&self, selector: RawFd) -> Result<(), Error> {
		{
			let mut guarded_data = self.data.lock().map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
				error
			})?;
			guarded_data.selector = Some(selector);
		}

		let mut write_buffers = vec![Arc::new(Mutex::new(LinkedList::new()))];
		let mut read_locks = vec![Arc::new(Mutex::new(0u128))];
		let cloned_guarded_data = self.data.clone();
		let cloned_callbacks = self.callbacks.clone();

		spawn(move || {
			let res = Self::poll_loop(
				&cloned_guarded_data,
				&cloned_callbacks,
				&mut write_buffers,
				&mut read_locks,
				selector,
			);
			match res {
				Ok(_) => {
					log!("poll_loop exited normally");
				}
				Err(e) => {
					log!("FATAL: Unexpected error in poll loop: {}", e.to_string());
				}
			}
		});

		Ok(())
	}

	fn process_handler_events(
		handler_events: Vec<HandlerEvent>,
		write_pending: Vec<RawFd>,
		evs: &mut Vec<GenericEvent>,
		read_fd_type: &mut Vec<FdType>,
		use_on_client_read: &mut Vec<bool>,
		write_buffers: &mut Vec<Arc<Mutex<LinkedList<WriteBuffer>>>>,
		guarded_data: &Arc<Mutex<GuardedData>>,
		on_close: Pin<Box<H>>,
		thread_pool: &StaticThreadPool,
	) -> Result<(), Error> {
		for handler_event in &handler_events {
			match handler_event.etype {
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
					let fd: Option<RawFd> = {
						let guarded_data = guarded_data.lock();
						match guarded_data {
							Ok(mut guarded_data) => guarded_data.seqno_map.remove(&seqno),
							Err(e) => {
								log!(
									"Unexpected error getting guarded data lock: {}",
									e.to_string()
								);
								None
							}
						}
					};
					match fd {
						Some(fd) => {
							evs.push(GenericEvent::new(fd, GenericEventType::DelRead));
							evs.push(GenericEvent::new(fd, GenericEventType::DelWrite));
							let fd = fd as uintptr_t;

							read_fd_type[handler_event.fd as usize] = FdType::Unknown;
							use_on_client_read[handler_event.fd as usize] = false;

							// delete any unwritten buffers
							if fd < write_buffers.len() {
								let mut linked_list =
									write_buffers[fd as usize].lock().map_err(|e| {
										let error: Error = ErrorKind::InternalError(format!(
											"Poison Error: {}",
											e
										))
										.into();
										error
									})?;

								let mut iter = (*linked_list).iter();
								loop {
									match iter.next() {
										Some(item) => drop(item),
										None => break,
									}
								}
								(*linked_list).clear();
							}

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
						None => {} // already deleted
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

	#[cfg(target_os = "linux")]
	fn get_events(
		_queue: RawFd,
		_input: Vec<GenericEvent>,
		_output: &mut Vec<GenericEvent>,
	) -> Result<i32, Error> {
		Ok(0)
	}

	#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
	fn get_events(
		queue: RawFd,
		input_events: Vec<GenericEvent>,
		output_events: &mut Vec<GenericEvent>,
	) -> Result<i32, Error> {
		let mut kevs = vec![];
		for ev in input_events {
			kevs.push(ev.to_kev());
		}

		let mut ret_kevs = vec![];
		for _ in 0..MAX_EVENTS_KQUEUE {
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
				MAX_EVENTS_KQUEUE,
				&duration_to_timespec(std::time::Duration::from_millis(1)),
			)
		};

		if ret_count < 0 {
			log!("Error in kevent: {:?}", std::io::Error::last_os_error());
		}

		let mut ret_count_adjusted = 0;
		for i in 0..ret_count {
			let kev = ret_kevs[i as usize];
			if !kev.flags.contains(EventFlag::EV_DELETE) {
				ret_count_adjusted += 1;
				if kev.filter == EVFILT_WRITE {
					output_events.push(GenericEvent::new(
						kev.ident as RawFd,
						GenericEventType::AddWriteET,
					));
				}
				if kev.filter == EVFILT_READ {
					output_events.push(GenericEvent::new(
						kev.ident as RawFd,
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
		write_buffers: &mut Vec<Arc<Mutex<LinkedList<WriteBuffer>>>>,
		read_locks: &mut Vec<Arc<Mutex<u128>>>,
		queue: RawFd,
	) -> Result<(), Error> {
		let thread_pool = StaticThreadPool::new()?;
		thread_pool.start(4)?;

		let accept_read_lock = Arc::new(RwLock::new(true));

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
		read_fd_type[rx as usize] = FdType::Wakeup;

		let mut use_on_client_read = Vec::new();
		// preallocate some
		use_on_client_read.reserve(INITIAL_MAX_FDS);
		for _ in 0..INITIAL_MAX_FDS {
			use_on_client_read.push(false);
		}

		// same for write_buffers
		for _ in 0..INITIAL_MAX_FDS {
			write_buffers.push(Arc::new(Mutex::new(LinkedList::new())));
		}

		// add the wakeup pipe rx here
		let mut output_events = vec![];
		let mut input_events = vec![];
		input_events.push(GenericEvent::new(rx, GenericEventType::AddReadLT));
		Self::get_events(queue, input_events, &mut output_events)?;

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
				write_buffers,
				guarded_data,
				on_close.clone(),
				&thread_pool,
			)?;

			let mut output_events = vec![];
			ret_count = Self::get_events(queue, evs, &mut output_events)?;

			// if no events are returned (on timeout), just bypass following logic and wait
			if ret_count == 0 {
				continue;
			}

			// first process write buffers in a single batch
			let mut fds = vec![];
			for event in &output_events {
				match event.etype {
					GenericEventType::AddWriteET => {
						fds.push(event.fd);
					}
					_ => {}
				}
			}

			let res = Self::process_write_buffers(fds, guarded_data, write_buffers);

			match res {
				Ok(_) => {}
				Err(e) => {
					log!(
						"Unexpected error in poll loop (write buffers): {}",
						e.to_string()
					);
				}
			}

			for event in output_events {
				if event.etype == GenericEventType::AddWriteET {
					let res = Self::process_event_write(
						event.fd as RawFd,
						&thread_pool,
						write_buffers,
						guarded_data,
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
						event.fd as RawFd,
						&read_fd_type,
						&thread_pool,
						guarded_data,
						on_read.clone(),
						on_accept.clone(),
						on_client_read.clone(),
						use_on_client_read[event.fd as usize],
						read_locks,
						write_buffers,
						seqno,
						&accept_read_lock,
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
	}

	fn process_write_buffers(
		fds: Vec<RawFd>,
		guarded_data: &Arc<Mutex<GuardedData>>,
		write_buffers: &mut Vec<Arc<Mutex<LinkedList<WriteBuffer>>>>,
	) -> Result<(), Error> {
		for fd in fds {
			let cur_len = write_buffers.len();
			if cur_len <= fd as usize {
				for _ in cur_len..(fd + 1) as usize {
					write_buffers.push(Arc::new(Mutex::new(LinkedList::new())));
				}
			}

			let mut linked_list = write_buffers[fd as usize].lock().map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
				error
			})?;
			loop {
				let mut guarded_data = guarded_data.lock().map_err(|e| {
					let error: Error =
						ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
					error
				})?;
				match guarded_data.write_buffers[fd as usize].pop_front() {
					Some(buffer) => linked_list.push_back(buffer),
					None => break,
				}
			}
		}

		Ok(())
	}

	fn write_loop(
		fd: RawFd,
		write_buffer: &mut WriteBuffer,
		guarded_data: &Arc<Mutex<GuardedData>>,
	) -> Result<u16, Error> {
		let initial_len = write_buffer.len;
		loop {
			let res = {
				let res = match guarded_data.lock() {
					Ok(data) => {
						let seqno = data.seqno_map.get(&write_buffer.connection_seqno);
						let res = match seqno {
							Some(resfd) => {
								if resfd != &fd {
									log!(
										"seqno = '{:?}' expected fd = '{}', found fd = '{}'",
										seqno,
										fd,
										resfd
									);
									return Err(ErrorKind::StaleFdError(format!(
										"write to closed fd: {}",
										fd
									))
									.into());
								}

								let res = write(
									fd,
									&write_buffer.buffer[(write_buffer.offset as usize)
										..(write_buffer.len as usize)],
								);
								res
							}
							None => {
								log!(
									"lookup seqno = {} expected fd = '{}', found fd = None",
									write_buffer.connection_seqno,
									fd
								);
								log!(
									"write_buffer.len = {}, write_buffer.offset = {}",
									write_buffer.len,
									write_buffer.offset
								);
								return Err(ErrorKind::StaleFdError(format!(
									"write to closed fd: {}",
									fd
								))
								.into());
							}
						};
						res
					}
					Err(e) => {
						log!("Unexpected error locking guarded_data {}", e.to_string());
						return Err(ErrorKind::InternalError(format!("poison error: {}", e)).into());
					}
				};

				res
			};

			match res {
				Ok(len) => {
					if len == (write_buffer.len as usize - write_buffer.offset as usize) {
						// we're done
						write_buffer.offset += len as u16;
						write_buffer.len -= len as u16;
						return Ok(initial_len);
					} else {
						// update values and write again
						write_buffer.offset += len as u16;
						write_buffer.len -= len as u16;
					}
				}
				Err(e) => {
					match e {
						Errno::EAGAIN => {
							// break because we're edge triggered.
							// a new event occurs.
							return Ok(initial_len - write_buffer.len);
						}
						_ => {
							// this is an actual write error.
							// close the connection.
							return Err(ErrorKind::ConnectionCloseError(format!(
								"connection closed: {}",
								e
							))
							.into());
						}
					}
				}
			}
		}
	}

	fn write_until_block(
		fd: RawFd,
		linked_list: &mut LinkedList<WriteBuffer>,
		guarded_data: &Arc<Mutex<GuardedData>>,
	) -> Result<(), Error> {
		loop {
			match (*linked_list).front_mut() {
				Some(front) => {
					let total_len = front.len;
					let ret = Self::write_loop(fd, front, guarded_data);
					match ret {
						Ok(len) => {
							if len == total_len {
								if front.close {
									Self::push_handler_event(
										fd,
										HandlerEventType::Close,
										guarded_data,
										false,
										front.connection_seqno,
									)?;
									let _ = close(fd);
								}
								(*linked_list).pop_front();
							} else {
								// we didn't complete, we need to break
								// we had to block so a new
								// edge triggered event will occur
								break;
							}
						}
						Err(e) => {
							log!("Error occurred: {}", e.to_string());
							Self::push_handler_event(
								fd,
								HandlerEventType::Close,
								guarded_data,
								false,
								front.connection_seqno,
							)?;
							let _ = close(fd);

							break;
						}
					}
				}
				None => {
					break;
				}
			}
		}

		Ok(())
	}

	fn process_event_write(
		fd: RawFd,
		thread_pool: &StaticThreadPool,
		write_buffers: &mut Vec<Arc<Mutex<LinkedList<WriteBuffer>>>>,
		guarded_data: &Arc<Mutex<GuardedData>>,
	) -> Result<(), Error> {
		let write_buffer_clone = write_buffers[fd as usize].clone();
		let guarded_data = guarded_data.clone();
		thread_pool
			.execute(async move {
				let write_buffer = write_buffer_clone.lock();
				match write_buffer {
					Ok(mut linked_list) => {
						let res = Self::write_until_block(fd, &mut linked_list, &guarded_data);

						match res {
							Ok(_) => {}
							Err(e) => {
								log!("unexpected error in process_event_write: {}", e.to_string());
							}
						}
					}
					Err(e) => log!(
						"unexpected error with locking write_buffer: {}",
						e.to_string()
					),
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
		fd: RawFd,
		read_fd_type: &Vec<FdType>,
		thread_pool: &StaticThreadPool,
		guarded_data: &Arc<Mutex<GuardedData>>,
		on_read: Pin<Box<F>>,
		on_accept: Pin<Box<G>>,
		on_client_read: Pin<Box<K>>,
		use_on_client_read: bool,
		read_locks: &mut Vec<Arc<Mutex<u128>>>,
		write_buffers: &mut Vec<Arc<Mutex<LinkedList<WriteBuffer>>>>,
		seqno: u128,
		accept_read_lock: &Arc<RwLock<bool>>,
	) -> Result<(), Error> {
		let fd_type = &read_fd_type[fd as usize];
		match fd_type {
			FdType::Listener => {
				let lock = accept_read_lock.write();
				match lock {
					Ok(_) => {}
					Err(e) => log!("Unexpected error obtaining read lock, {}", e),
				}
				let res = accept(fd);
				match res {
					Ok(res) => {
						if write_buffers.len() > res as usize {
							// if it's not, it will be allocated on write
							// and there's nothing to delete here
							match write_buffers[res as usize].lock() {
								Ok(mut list) => {
									list.clear();
								}
								Err(e) => {
									log!(
										"unexpected error retreiving linkedlist, {}",
										e.to_string(),
									);
								}
							}
						}

						if read_locks.len() <= res as usize {
							Self::check_and_set(
								read_locks,
								res as usize,
								Arc::new(Mutex::new(seqno)),
							);
						}

						{
							let current_seqno = read_locks[res as usize].lock();
							match current_seqno {
								Ok(mut current_seqno) => {
									*current_seqno = seqno;
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

						// insert the seqno into the hashmap
						{
							let guarded_data = guarded_data.lock();
							match guarded_data {
								Ok(mut guarded_data) => {
									guarded_data.seqno_map.insert(seqno, res);
								}
								Err(e) => {
									log!("Error getting guarded data: {}", e.to_string());
								}
							}
						}

						// set non-blocking
						fcntl(res, F_SETFL(OFlag::from_bits(libc::O_NONBLOCK).unwrap())).map_err(
							|e| {
								let error: Error =
									ErrorKind::InternalError(format!("fcntl error: {}", e)).into();
								error
							},
						)?;
						let guarded_data = guarded_data.clone();

						let accept_res = Self::process_accept_result(fd, res, &guarded_data);
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
					}
					Err(e) => Self::process_accept_err(fd, e),
				}?;
			}
			FdType::Stream => {
				let guarded_data = guarded_data.clone();
				let read_lock = read_locks[fd as usize].clone();
				let accept_read_lock = accept_read_lock.clone();
				thread_pool
					.execute(async move {
						let lock = accept_read_lock.read();
						match lock {
							Ok(_) => {}
							Err(e) => log!("Unexpected error obtaining read lock: {}", e),
						}
						let mut buf = [0u8; BUFFER_SIZE];
						loop {
							let lock = read_lock.lock();
							let seqno = match lock {
								Ok(seqno) => seqno,
								Err(e) => {
									log!("Unexpected Error obtaining read lock: {}", e.to_string());
									break;
								}
							};
							let res = read(fd, &mut buf);
							match res {
								Ok(res) => {
									let _ = Self::process_read_result(
										fd,
										res,
										buf,
										&guarded_data,
										on_read.clone(),
										on_client_read.clone(),
										use_on_client_read,
										*seqno,
									);
								}
								Err(e) => {
									let _ = Self::process_read_err(fd, e, &guarded_data, *seqno);
									break;
								}
							};
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
				read(fd, &mut [0u8; 1]).map_err(|e| {
					let error: Error = ErrorKind::InternalError(format!(
						"Error reading from pipe, {}",
						e.to_string()
					))
					.into();
					error
				})?;
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
		guarded_data: &Arc<Mutex<GuardedData>>,
		on_read: Pin<Box<F>>,
		on_client_read: Pin<Box<K>>,
		use_on_client_read: bool,
		connection_seqno: u128,
	) -> Result<(), Error> {
		if len > 0 {
			// build write handle
			let wh = WriteHandle::new(fd, guarded_data.clone(), connection_seqno);

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
				false,
				connection_seqno,
			)?;
			let _ = close(fd);
		}
		Ok(())
	}

	fn process_read_err(
		fd: RawFd,
		error: Errno,
		guarded_data: &Arc<Mutex<GuardedData>>,
		connection_seqno: u128,
	) -> Result<(), Error> {
		// don't close if it's an EAGAIN or one of the other non-terminal errors
		match error {
			Errno::EAGAIN => {
				// we do nothing here.
				// the edge triggered event will occur again and
				// we'll read from the main loop
			}
			_ => {
				Self::push_handler_event(
					fd,
					HandlerEventType::Close,
					guarded_data,
					false,
					connection_seqno,
				)?;
				let _ = close(fd);
			}
		}
		Ok(())
	}

	fn process_accept_result(
		_acceptor: RawFd,
		nfd: RawFd,
		guarded_data: &Arc<Mutex<GuardedData>>,
	) -> Result<(), Error> {
		Self::push_handler_event(nfd, HandlerEventType::Accept, guarded_data, false, 0).map_err(
			|e| {
				let error: Error = ErrorKind::InternalError(format!(
					"push handler event error: {}",
					e.to_string()
				))
				.into();
				error
			},
		)?;
		Ok(())
	}

	fn process_accept_err(_acceptor: RawFd, error: Errno) -> Result<(), Error> {
		log!("error on acceptor: {}", error);
		Ok(())
	}

	fn push_handler_event(
		fd: RawFd,
		event_type: HandlerEventType,
		guarded_data: &Arc<Mutex<GuardedData>>,
		wakeup: bool,
		seqno: u128,
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
				write(wakeup_fd, &[0u8; 1])?;
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
