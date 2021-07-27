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

// macos deps

#[cfg(target_os = "macos")]
use kqueue_sys::EventFilter::{self, EVFILT_READ, EVFILT_WRITE};
#[cfg(target_os = "macos")]
use kqueue_sys::{kevent, kqueue, EventFlag, FilterFlag};

use crate::duration_to_timespec;
use crate::util::threadpool::ThreadPool;
use crate::util::{Error, ErrorKind};
use libc::uintptr_t;
use nioruntime_libnio::ActionType;
use nioruntime_util::log;
use nix::errno::Errno;
use nix::fcntl::fcntl;
use nix::fcntl::OFlag;
use nix::fcntl::F_SETFL;
use nix::sys::socket::accept;
use nix::unistd::close;
use nix::unistd::pipe;
use nix::unistd::read;
use nix::unistd::write;
use rand::thread_rng;
use rand::Rng;
use std::collections::LinkedList;
use std::convert::TryInto;
use std::net::TcpListener;
use std::net::TcpStream;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::thread::spawn;
use std::time::Duration;

const INITIAL_MAX_FDS: usize = 100;
const BUFFER_SIZE: usize = 1024;
const MAX_EVENTS_KQUEUE: i32 = 100;

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
	ResumeRead,
	ResumeWrite,
}

#[derive(Clone, PartialEq, Debug)]
struct HandlerEvent {
	etype: HandlerEventType,
	fd: RawFd,
}

impl HandlerEvent {
	fn new(fd: RawFd, etype: HandlerEventType) -> Self {
		HandlerEvent { fd, etype }
	}
}

#[derive(Clone, Debug, PartialEq)]
enum FdType {
	Wakeup,
	Listener,
	Stream,
	PausedStream,
	Unknown,
}

struct WriteBuffer<I, J> {
	offset: u16,
	len: u16,
	buffer: [u8; BUFFER_SIZE],
	msg_id: u128,
	on_write_success: Option<I>,
	on_write_fail: Option<J>,
}

struct Callbacks<F, G, H, I, J, K> {
	on_read: Option<Pin<Box<F>>>,
	on_accept: Option<Pin<Box<G>>>,
	on_close: Option<Pin<Box<H>>>,
	on_write_success: Option<Pin<Box<I>>>,
	on_write_fail: Option<Pin<Box<J>>>,
	on_client_read: Option<Pin<Box<K>>>,
}

#[derive(PartialEq, Debug)]
enum ConnectionState {
	Ok,
	Closing,
}

impl Default for ConnectionState {
	fn default() -> Self {
		ConnectionState::Ok
	}
}

struct GuardedData<F, G, H, I, J, K> {
	fd_actions: Vec<RawFdAction>,
	wakeup_fd: RawFd,
	wakeup_rx: RawFd,
	wakeup_scheduled: bool,
	connection_state: Vec<ConnectionState>,
	handler_events: Vec<HandlerEvent>,
	write_pending: Vec<RawFd>,
	write_buffers: Vec<LinkedList<WriteBuffer<Pin<Box<I>>, Pin<Box<J>>>>>,
	queue: Option<RawFd>,
	callbacks: Callbacks<F, G, H, I, J, K>,
}

pub struct KqueueEventHandler<F, G, H, I, J, K> {
	data: Arc<Mutex<GuardedData<F, G, H, I, J, K>>>,
}

impl<F, G, H, I, J, K> KqueueEventHandler<F, G, H, I, J, K>
where
	F: Fn(u128, u128, &[u8], usize) -> Result<(&[u8], usize, usize), Error>
		+ Send
		+ 'static
		+ Clone
		+ Sync
		+ Unpin,
	G: Fn(u128) -> Result<(), Error> + Send + 'static + Clone + Sync,
	H: Fn(u128) -> Result<(), Error> + Send + 'static + Clone + Sync,
	I: Fn(u128, u128) -> Result<(), Error> + Send + 'static + Clone + Sync,
	J: Fn(u128, u128) -> Result<(), Error> + Send + 'static + Clone + Sync,
	K: Fn(u128, u128, &[u8], usize) -> Result<(&[u8], usize, usize), Error>
		+ Send
		+ 'static
		+ Clone
		+ Sync
		+ Unpin,
{
	pub fn add_tcp_stream(&mut self, stream: &TcpStream) -> Result<i32, Error> {
		// make sure we have a client on_read handler configured
		{
			let data = self.data.lock().map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
				error
			})?;

			match data.callbacks.on_read {
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
		#[cfg(any(unix, macos))]
		let ret = self.add_fd(stream.as_raw_fd(), ActionType::AddStream)?;
		#[cfg(target_os = "windows")]
		let ret = self.add_socket(stream.as_raw_socket(), ActionType::AddStream)?;
		Ok(ret)
	}

	pub fn add_tcp_listener(&mut self, listener: &TcpListener) -> Result<i32, Error> {
		// must be nonblocking
		listener.set_nonblocking(true)?;
		#[cfg(any(unix, macos))]
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

		if data.queue.is_none() {
			return Err(ErrorKind::SetupError("queue must be started first".to_string()).into());
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
		let data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		match data.callbacks.on_read {
			Some(_) => {}
			None => {
				return Err(ErrorKind::SetupError(
					"on_read callback must be registered first".to_string(),
				)
				.into());
			}
		}

		match data.callbacks.on_accept {
			Some(_) => {}
			None => {
				return Err(ErrorKind::SetupError(
					"on_accept callback must be registered first".to_string(),
				)
				.into());
			}
		}

		match data.callbacks.on_close {
			Some(_) => {}
			None => {
				return Err(ErrorKind::SetupError(
					"on_close callback must be registered first".to_string(),
				)
				.into());
			}
		}

		match data.callbacks.on_write_fail {
			Some(_) => {}
			None => {
				return Err(ErrorKind::SetupError(
					"on_write_fail callback must be registered first".to_string(),
				)
				.into());
			}
		}

		match data.callbacks.on_write_success {
			Some(_) => {}
			None => {
				return Err(ErrorKind::SetupError(
					"on_write_success callback must be registered first".to_string(),
				)
				.into());
			}
		}

		Ok(())
	}

	fn do_wakeup_with_lock(
		data: &mut GuardedData<F, G, H, I, J, K>,
	) -> Result<(RawFd, bool), Error> {
		let wakeup_scheduled = data.wakeup_scheduled;
		if !wakeup_scheduled {
			data.wakeup_scheduled = true;
		}
		Ok((data.wakeup_fd, wakeup_scheduled))
	}

	fn do_wakeup(data: &Arc<Mutex<GuardedData<F, G, H, I, J, K>>>) -> Result<(), Error> {
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

	fn value_of<T>(vec: &Vec<T>, i: usize) -> Option<&T>
	where
		T: Default,
	{
		match vec.len() > i {
			true => Some(&vec[i]),
			false => None,
		}
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

	fn write(
		id: i32,
		data: &[u8],
		offset: usize,
		len: usize,
		guarded_data: &Arc<Mutex<GuardedData<F, G, H, I, J, K>>>,
		msg_id: u128,
		on_write_success: Pin<Box<I>>,
		on_write_fail: Pin<Box<J>>,
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
					msg_id,
					on_write_success: match rem <= BUFFER_SIZE {
						true => Some(on_write_success.clone()),
						false => None,
					},
					on_write_fail: match rem <= BUFFER_SIZE {
						true => Some(on_write_fail.clone()),
						false => None,
					},
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

		guarded_data.callbacks.on_read = Some(Box::pin(on_read));

		Ok(())
	}

	pub fn set_on_accept(&mut self, on_accept: G) -> Result<(), Error> {
		let mut guarded_data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		guarded_data.callbacks.on_accept = Some(Box::pin(on_accept));

		Ok(())
	}

	pub fn set_on_close(&mut self, on_close: H) -> Result<(), Error> {
		let mut guarded_data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		guarded_data.callbacks.on_close = Some(Box::pin(on_close));

		Ok(())
	}

	pub fn set_on_write_success(&mut self, on_write_success: I) -> Result<(), Error> {
		let mut guarded_data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		guarded_data.callbacks.on_write_success = Some(Box::pin(on_write_success));

		Ok(())
	}

	pub fn set_on_write_fail(&mut self, on_write_fail: J) -> Result<(), Error> {
		let mut guarded_data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		guarded_data.callbacks.on_write_fail = Some(Box::pin(on_write_fail));

		Ok(())
	}

	pub fn set_on_client_read(&mut self, on_client_read: K) -> Result<(), Error> {
		let mut guarded_data = self.data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		guarded_data.callbacks.on_client_read = Some(Box::pin(on_client_read));

		Ok(())
	}

	pub fn new() -> Self {
		// create the pipe (for wakeups)
		let (rx, tx) = pipe().unwrap();

		let callbacks = Callbacks {
			on_read: None,
			on_accept: None,
			on_close: None,
			on_write_success: None,
			on_write_fail: None,
			on_client_read: None,
		};

		let guarded_data = GuardedData {
			fd_actions: vec![],
			wakeup_fd: tx,
			wakeup_rx: rx,
			wakeup_scheduled: false,
			connection_state: vec![],
			handler_events: vec![],
			write_pending: vec![],
			write_buffers: vec![LinkedList::new()],
			queue: None,
			callbacks,
		};
		let guarded_data = Arc::new(Mutex::new(guarded_data));

		KqueueEventHandler { data: guarded_data }
	}

	#[cfg(target_os = "linux")]
	pub fn start(&self) -> Result<(), Error> {
		Ok(())
	}

	#[cfg(target_os = "macos")]
	pub fn start(&self) -> Result<(), Error> {
		// create the kqueue
		let queue = unsafe { kqueue() };
		{
			let mut guarded_data = self.data.lock().map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
				error
			})?;
			guarded_data.queue = Some(queue);
		}

		let mut write_buffers = vec![Arc::new(Mutex::new(LinkedList::new()))];
		let cloned_guarded_data = self.data.clone();

		spawn(move || {
			let res = Self::poll_loop(&cloned_guarded_data, &mut write_buffers, queue);
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

	#[cfg(target_os = "macos")]
	fn contained_event_for(
		fd: uintptr_t,
		event_type: EventFilter,
		ret_kevs: &Vec<kevent>,
		ret_count: i32,
	) -> bool {
		for i in 0..ret_count {
			let kev = ret_kevs[i as usize];
			if kev.ident == fd && kev.filter == event_type {
				return true;
			}
		}
		false
	}

	#[cfg(target_os = "macos")]
	fn process_handler_events(
		handler_events: Vec<HandlerEvent>,
		write_pending: Vec<RawFd>,
		kevs: &mut Vec<kevent>,
		ret_kevs: &Vec<kevent>,
		ret_count: i32,
		read_fd_type: &mut Vec<FdType>,
		use_on_client_read: &mut Vec<bool>,
		write_buffers: &mut Vec<Arc<Mutex<LinkedList<WriteBuffer<Pin<Box<I>>, Pin<Box<J>>>>>>>,
	) -> Result<(), Error> {
		for handler_event in &handler_events {
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

					let len = use_on_client_read.len();
					if fd >= len {
						for _ in len..fd + 1 {
							use_on_client_read.push(false);
						}
					}
					use_on_client_read[fd] = false;
				}
				HandlerEventType::Close => {
					let fd = handler_event.fd as uintptr_t;
					kevs.push(kevent::new(
						fd,
						EventFilter::EVFILT_READ,
						EventFlag::EV_DELETE,
						FilterFlag::empty(),
					));
					kevs.push(kevent::new(
						fd,
						EventFilter::EVFILT_WRITE,
						EventFlag::EV_DELETE,
						FilterFlag::empty(),
					));
					read_fd_type[handler_event.fd as usize] = FdType::Unknown;
					use_on_client_read[handler_event.fd as usize] = false;

					// delete any unwritten buffers
					if fd < write_buffers.len() {
						let mut linked_list = write_buffers[fd as usize].lock().map_err(|e| {
							let error: Error =
								ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
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
				}
				HandlerEventType::ResumeRead => {
					let fd = handler_event.fd as uintptr_t;
					if !Self::contained_event_for(fd, EVFILT_READ, ret_kevs, ret_count) {
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
					}
				}
				HandlerEventType::ResumeWrite => {
					let fd = handler_event.fd as uintptr_t;
					if !Self::contained_event_for(fd, EVFILT_WRITE, ret_kevs, ret_count) {
						kevs.push(kevent::new(
							fd,
							EventFilter::EVFILT_WRITE,
							EventFlag::EV_ADD,
							FilterFlag::empty(),
						));
					}
				}
			}
		}

		// remove events that have already been processed (will be added back in with handler events)
		// this ensures only a single event is being processed at a time
		let mut i = 0;
		for kev in ret_kevs {
			if i >= ret_count {
				break;
			}
			i += 1;

			// check that we didn't already receive the handler event for this kev
			// TODO: could be perf issue O(n X m)
			let mut found_collision = false;
			for handler_event in &handler_events {
				if handler_event.fd == kev.ident.try_into().unwrap()
					&& ((handler_event.etype == HandlerEventType::ResumeWrite
						&& kev.filter == EVFILT_WRITE)
						|| (handler_event.etype == HandlerEventType::ResumeRead
							&& kev.filter == EVFILT_READ))
				{
					found_collision = true;
					break;
				}
			}

			// since there's a collision, we don't need to delete the event
			if found_collision {
				continue;
			}

			if read_fd_type[kev.ident] == FdType::Stream
				|| read_fd_type[kev.ident] == FdType::PausedStream
			{
				match kev.filter {
					EVFILT_READ => kevs.push(kevent::new(
						kev.ident,
						EventFilter::EVFILT_READ,
						EventFlag::EV_DELETE,
						FilterFlag::empty(),
					)),
					EVFILT_WRITE => kevs.push(kevent::new(
						kev.ident,
						EventFilter::EVFILT_WRITE,
						EventFlag::EV_DELETE,
						FilterFlag::empty(),
					)),
					_ => {
						log!("Unexpected event type: {:?}", kev);
					}
				}
			}
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
		Ok(())
	}

	#[cfg(target_os = "macos")]
	fn poll_loop(
		guarded_data: &Arc<Mutex<GuardedData<F, G, H, I, J, K>>>,
		write_buffers: &mut Vec<Arc<Mutex<LinkedList<WriteBuffer<Pin<Box<I>>, Pin<Box<J>>>>>>>,
		queue: RawFd,
	) -> Result<(), Error> {
		let thread_pool = ThreadPool::new(4)?;

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

		let mut ret_kevs = vec![];
		for _ in 0..MAX_EVENTS_KQUEUE {
			ret_kevs.push(kevent::new(
				0,
				EventFilter::EVFILT_SYSCOUNT,
				EventFlag::empty(),
				FilterFlag::empty(),
			));
		}

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
				ret_kevs.as_mut_ptr(),
				MAX_EVENTS_KQUEUE,
				&duration_to_timespec(Duration::from_millis(1)),
			)
		};

		let mut ret_count = 0;
		loop {
			let to_process;
			let handler_events;
			let write_pending;
			let on_read;
			let on_accept;
			let on_close;
			let on_write_success;
			let on_write_fail;
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
				on_read = guarded_data.callbacks.on_read.as_ref().unwrap().clone();
				on_accept = guarded_data.callbacks.on_accept.as_ref().unwrap().clone();
				on_close = guarded_data.callbacks.on_close.as_ref().unwrap().clone();
				on_write_success = guarded_data
					.callbacks
					.on_write_success
					.as_ref()
					.unwrap()
					.clone();
				on_write_fail = guarded_data
					.callbacks
					.on_write_fail
					.as_ref()
					.unwrap()
					.clone();
				on_client_read = guarded_data
					.callbacks
					.on_client_read
					.as_ref()
					.unwrap()
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

						let len = use_on_client_read.len();
						if fd >= len {
							for _ in len..fd + 1 {
								use_on_client_read.push(false);
							}
						}
						use_on_client_read[fd] = true;
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

						let len = use_on_client_read.len();
						if fd >= len {
							for _ in len..fd + 1 {
								use_on_client_read.push(false);
							}
						}
						use_on_client_read[fd] = false;
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
			Self::process_handler_events(
				handler_events,
				write_pending,
				&mut kevs,
				&ret_kevs,
				ret_count,
				&mut read_fd_type,
				&mut use_on_client_read,
				write_buffers,
			)?;

			ret_count = unsafe {
				kevent(
					queue,
					kevs.as_ptr(),
					kevs.len() as i32,
					ret_kevs.as_mut_ptr(),
					MAX_EVENTS_KQUEUE,
					&duration_to_timespec(Duration::from_millis(100)),
				)
			};

			// handle error here
			if ret_count < 0 {
				match Self::process_error() {
					Ok(_) => {}
					Err(e) => {
						log!("Unexpected error in poll loop: {}", e.to_string());
					}
				}
				continue;
			}

			// if no events are returned (on timeout), just bypass following logic and wait
			if ret_count == 0 {
				continue;
			}
			// check if we have all delete events which can be ignored
			let mut has_non_delete = false;
			let mut i = 0;
			for k in &ret_kevs {
				if !k.flags.contains(EventFlag::EV_DELETE) {
					has_non_delete = true;
					break;
				}
				i += 1;
				if i >= ret_count {
					break;
				}
			}
			if !has_non_delete {
				continue;
			}

			// first process write buffers in a single batch
			let mut fds = vec![];
			for i in 0..ret_count {
				let kev = ret_kevs[i as usize];
				if kev.filter == EVFILT_WRITE {
					fds.push(kev.ident as RawFd);
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

			for i in 0..ret_count {
				let kev = ret_kevs[i as usize];
				if kev.filter == EVFILT_WRITE && !kev.flags.contains(EventFlag::EV_DELETE) {
					let res = Self::process_event_write(
						kev.ident as RawFd,
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
				if kev.filter == EVFILT_READ {
					let res = Self::process_event_read(
						kev.ident as RawFd,
						&read_fd_type,
						&thread_pool,
						guarded_data,
						on_read.clone(),
						on_accept.clone(),
						on_close.clone(),
						on_write_success.clone(),
						on_write_fail.clone(),
						on_client_read.clone(),
						use_on_client_read[kev.ident as usize],
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

	fn process_error() -> Result<(), Error> {
		log!("Error in event queue, {}", std::io::Error::last_os_error());
		Ok(())
	}

	fn process_write_buffers(
		fds: Vec<RawFd>,
		guarded_data: &Arc<Mutex<GuardedData<F, G, H, I, J, K>>>,
		write_buffers: &mut Vec<Arc<Mutex<LinkedList<WriteBuffer<Pin<Box<I>>, Pin<Box<J>>>>>>>,
	) -> Result<(), Error> {
		let mut guarded_data = guarded_data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

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
				match guarded_data.write_buffers[fd as usize].pop_front() {
					Some(buffer) => linked_list.push_back(buffer),
					None => break,
				}
			}
		}

		Ok(())
	}

	fn process_event_write(
		fd: RawFd,
		thread_pool: &ThreadPool,
		write_buffers: &mut Vec<Arc<Mutex<LinkedList<WriteBuffer<Pin<Box<I>>, Pin<Box<J>>>>>>>,
		guarded_data: &Arc<Mutex<GuardedData<F, G, H, I, J, K>>>,
	) -> Result<(), Error> {
		let write_buffer_clone = write_buffers[fd as usize].clone();
		let guarded_data_clone = guarded_data.clone();

		thread_pool
			.execute(async move {
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
												Some(write_buffer) => {
													// check if there's a write success
													// handler and call it if so
													match write_buffer.on_write_success {
														Some(h) => {
															let hres = (h)(
																fd.try_into().unwrap_or(0),
																write_buffer.msg_id,
															);
															match hres {
																Ok(_) => {},
																Err(e) => log!("on_write_success callback resulted in: {}", e.to_string()),
															}
														}
														None => {}
													}
												}
												None => {
													log!("unexpected error couldn't pop");
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
														log!(
															"handler push err: {}",
															e.to_string()
														)
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
													log!("handler push err: {}", e.to_string())
												}
											}
										}
									}
									Err(e) => {
										log!("write error: {}", e.to_string());
										loop {
											if (*linked_list).is_empty() {
												break;
											}
											match (*linked_list).pop_front() {
												Some(item) => match item.on_write_fail {
													Some(h) => {
														let hres = (h)(
															fd.try_into().unwrap_or(0),
															item.msg_id,
														);
														match hres {
                                                                                                                	Ok(_) => {},
                                                                                                                	Err(e) => log!("on_write_fail callback resulted in: {}", e.to_string()),
                                                                                                                }
													}
													None => {}
												},
												None => {}
											}
										}
										(*linked_list).clear();
									}
								}
							}
							None => {
								log!("unepxected none");
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
		thread_pool: &ThreadPool,
		guarded_data: &Arc<Mutex<GuardedData<F, G, H, I, J, K>>>,
		on_read: Pin<Box<F>>,
		on_accept: Pin<Box<G>>,
		on_close: Pin<Box<H>>,
		on_write_success: Pin<Box<I>>,
		on_write_fail: Pin<Box<J>>,
		on_client_read: Pin<Box<K>>,
		use_on_client_read: bool,
	) -> Result<(), Error> {
		let fd_type = &read_fd_type[fd as usize];
		match fd_type {
			FdType::Listener => {
				// one at a time per fd
				let res = accept(fd);
				match res {
					Ok(res) => {
						// set non-blocking
						fcntl(res, F_SETFL(OFlag::from_bits(libc::O_NONBLOCK).unwrap())).map_err(
							|e| {
								let error: Error =
									ErrorKind::InternalError(format!("fcntl error: {}", e)).into();
								error
							},
						)?;
						let accept_res = (on_accept)(res as u128);
						match accept_res {
							Ok(_) => {}
							Err(e) => log!("on_accept callback resulted in: {}", e.to_string()),
						}
						Self::process_accept_result(fd, res, guarded_data)
					}
					Err(e) => Self::process_accept_err(fd, e),
				}?;
			}
			FdType::Stream | FdType::PausedStream => {
				// note that a PausedStream may still get a read event to close,
				// so we process it
				let guarded_data = guarded_data.clone();
				let fd_type = fd_type.clone();
				thread_pool
					.execute(async move {
						let mut buf = [0u8; BUFFER_SIZE];
						// in order to ensure sequence in tact, obtain the lock
						let res = read(fd, &mut buf);
						let _ = match res {
							Ok(res) => Self::process_read_result(
								fd,
								res,
								buf,
								&guarded_data,
								on_read,
								on_close,
								on_write_success,
								on_write_fail,
								on_client_read,
								use_on_client_read,
								fd_type,
							),
							Err(e) => {
								Self::process_read_err(fd, e, &guarded_data, on_close, fd_type)
							}
						};
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
		guarded_data: &Arc<Mutex<GuardedData<F, G, H, I, J, K>>>,
		on_read: Pin<Box<F>>,
		on_close: Pin<Box<H>>,
		on_write_success: Pin<Box<I>>,
		on_write_fail: Pin<Box<J>>,
		on_client_read: Pin<Box<K>>,
		use_on_client_read: bool,
		fd_type: FdType,
	) -> Result<(), Error> {
		if len > 0 {
			if fd_type == FdType::PausedStream {
				log!("unexpected read on paused stream: {}", fd);
			}
			let msg_id: u128 = thread_rng().gen::<u128>();
			let result = match use_on_client_read {
				//let (resp, offset, len) = match use_on_client_read {
				true => (on_client_read)(fd.try_into().unwrap_or(0), msg_id, &buf, len),
				false => (on_read)(fd.try_into().unwrap_or(0), msg_id, &buf, len),
			};

			match result {
				Ok((resp, offset, len)) => {
					if len > 0 {
						Self::write(
							fd,
							resp,
							offset,
							len,
							guarded_data,
							msg_id,
							on_write_success,
							on_write_fail,
						)?;
						Self::push_handler_event(
							fd,
							HandlerEventType::ResumeRead,
							guarded_data,
							true,
						)?;
					}
				}
				Err(e) => {
					log!("Client callback resulted in error: {}", e.to_string());
				}
			}
		} else {
			// close
			let is_dup =
				Self::push_handler_event(fd, HandlerEventType::Close, guarded_data, false)?;
			if !is_dup {
				let _ = close(fd);
				let close_res = (on_close)(fd.try_into().unwrap_or(0));
				match close_res {
					Ok(_) => {}
					Err(e) => log!("on close callback resulted in error: {}", e.to_string()),
				}
			}
		}
		Ok(())
	}

	fn process_read_err(
		fd: RawFd,
		error: Errno,
		guarded_data: &Arc<Mutex<GuardedData<F, G, H, I, J, K>>>,
		on_close: Pin<Box<H>>,
		_fd_type: FdType,
	) -> Result<(), Error> {
		// don't close if it's an EAGAIN or one of the other non-terminal errors
		match error {
			Errno::EAGAIN => {
				Self::push_handler_event(fd, HandlerEventType::ResumeRead, guarded_data, false)?;
			}
			_ => {
				let is_dup =
					Self::push_handler_event(fd, HandlerEventType::Close, guarded_data, false)?;
				if !is_dup {
					let _ = close(fd);
					let res = (on_close)(fd.try_into().unwrap_or(0));
					match res {
						Ok(_) => {}
						Err(e) => log!("on_close callback resulted in: {}", e.to_string()),
					}
				}
			}
		}
		Ok(())
	}

	fn process_accept_result(
		_acceptor: RawFd,
		nfd: RawFd,
		guarded_data: &Arc<Mutex<GuardedData<F, G, H, I, J, K>>>,
	) -> Result<(), Error> {
		Self::push_handler_event(nfd, HandlerEventType::Accept, guarded_data, false).map_err(
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
		guarded_data: &Arc<Mutex<GuardedData<F, G, H, I, J, K>>>,
		wakeup: bool,
	) -> Result<bool, Error> {
		let mut is_dup = false;
		{
			let guarded_data = guarded_data.lock();
			let mut wakeup_fd = 0;
			let mut wakeup_scheduled = false;
			match guarded_data {
				Ok(mut guarded_data) => {
					// check for duplicates
					let nevent = HandlerEvent::new(fd, event_type.clone());
					is_dup = guarded_data
						.handler_events
						.iter()
						.any(|event| event.etype == nevent.etype && event.fd == nevent.fd);

					match Self::value_of(&guarded_data.connection_state, fd as usize) {
						Some(state) => {
							if *state == ConnectionState::Closing {
								is_dup = true;
							}
						}
						None => {}
					}

					// mark this connection for future iterations
					match event_type {
						HandlerEventType::Close => {
							Self::check_and_set(
								&mut guarded_data.connection_state,
								fd as usize,
								ConnectionState::Closing,
							);
						}
						_ => Self::check_and_set(
							&mut guarded_data.connection_state,
							fd as usize,
							ConnectionState::Ok,
						),
					}

					if !is_dup || event_type != HandlerEventType::Close {
						guarded_data.handler_events.push(nevent);
					}

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
		Ok(is_dup)
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
	let mut kqe = KqueueEventHandler::new();

	// echo
	kqe.set_on_read(|_, _, buf: &[u8], len| Ok((buf, 0, len)))?;

	kqe.set_on_accept(|_| Ok(()))?;
	kqe.set_on_close(|_| Ok(()))?;
	kqe.set_on_write_success(|_, _| Ok(()))?;
	kqe.set_on_write_fail(|_, _| Ok(()))?;
	kqe.set_on_client_read(move |_connection_id, _message_id, buf: &[u8], len| {
		assert_eq!(len, 5);
		assert_eq!(buf[0], 1);
		assert_eq!(buf[1], 2);
		assert_eq!(buf[2], 3);
		assert_eq!(buf[3], 4);
		assert_eq!(buf[4], 5);
		let mut x = x.lock().unwrap();
		(*x) += 5;
		Ok((buf, 0, 0))
	})?;

	kqe.start()?;

	kqe.add_tcp_listener(&listener)?;
	kqe.add_tcp_stream(&stream)?;

	stream.write(&[1, 2, 3, 4, 5])?;
	// wait long enough to make sure the client got the message
	std::thread::sleep(std::time::Duration::from_millis(100));
	let x = x_clone.lock().unwrap();
	assert_eq!((*x), 5);
	Ok(())
}
