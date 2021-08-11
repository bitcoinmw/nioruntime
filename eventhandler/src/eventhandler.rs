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

debug!();

#[cfg(unix)]
type ConnectionHandle = i32;
#[cfg(target_os = "windows")]
type ConnectionHandle = u64;

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
#[cfg(target_os = "windows")]
const WINSOCK_BUF_SIZE: winapi::c_int = 100_000_000;

#[derive(Debug, Clone)]
pub(crate) enum ActionType {
	AddStream,
	AddListener,
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
	fd: ConnectionHandle,
	etype: GenericEventType,
}

impl GenericEvent {
	fn new(fd: ConnectionHandle, etype: GenericEventType) -> Self {
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

fn do_write_no_fd_lock(
	fd: ConnectionHandle,
	connection_seqno: u128,
	data: &[u8],
	close: bool,
	guarded_data: &Arc<Mutex<GuardedData>>,
) -> Result<(), Error> {
	let mut buf = [0u8; BUFFER_SIZE];
	let len = data.len();
	let mut rem = len;
	let mut start = 0;
	let mut end = if len > BUFFER_SIZE { BUFFER_SIZE } else { len };
	let mut wakeupfd = 0;
	let mut wakeup_scheduled = false;
	match guarded_data.lock() {
		Ok(mut guarded_data) => loop {
			let len = if rem < BUFFER_SIZE { rem } else { BUFFER_SIZE };
			buf[..len].clone_from_slice(&data[start..end]);
			guarded_data.write_queue.push(StreamWriteBuffer {
				fd,
				write_buffer: WriteBuffer {
					buffer: buf.clone(),
					offset: 0,
					len: len.try_into().unwrap_or(0),
					close,
					connection_seqno,
				},
			});
			if rem <= BUFFER_SIZE {
				let (fd_inner, wakeup_scheduled_inner) = do_wakeup_with_lock(&mut *guarded_data)?;
				wakeupfd = fd_inner;
				wakeup_scheduled = wakeup_scheduled_inner;
				break;
			}
			rem -= BUFFER_SIZE;
			start += BUFFER_SIZE;
			end += BUFFER_SIZE;
			if end > data.len() {
				end = data.len();
			}
		},
		Err(e) => {
			info!("unexpected error obtaining guarded_data lock, {}", e);
		}
	}
	if !wakeup_scheduled {
		#[cfg(unix)]
		{
			let buf: *mut c_void = &mut [0u8; 1] as *mut _ as *mut c_void;
			unsafe {
				write(wakeupfd, buf, 1);
			}
		}
		#[cfg(target_os = "windows")]
		{
			let buf: *mut i8 = &mut [0i8; 1] as *mut _ as *mut i8;
			unsafe {
				ws2_32::send(wakeupfd, buf, 1, 0);
			}
		}
	}
	Ok(())
}

fn do_write(
	fd: ConnectionHandle,
	data: &[u8],
	offset: usize,
	len: usize,
	close: bool,
	fd_lock: Option<&Arc<Mutex<StateInfo>>>,
	connection_id: u128,
	guarded_data: &Arc<Mutex<GuardedData>>,
) -> Result<(), Error> {
	if len + offset > data.len() {
		return Err(ErrorKind::ArrayIndexOutofBounds(format!(
			"offset+len='{}',data.len='{}'",
			offset + len,
			data.len()
		))
		.into());
	}
	if fd_lock.is_none() {
		do_write_no_fd_lock(fd, connection_id, data, close, guarded_data)?;
		return Ok(());
	} else {
		let fd_lock = fd_lock.unwrap();
		let linked_list = &mut fd_lock
			.lock()
			.map_err(|e| {
				let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
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
				connection_seqno: connection_id,
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
		let mut guarded_data = guarded_data.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		guarded_data.write_pending.push(fd.into());
		do_wakeup_with_lock(&mut *guarded_data)?
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
				ws2_32::send(fd, buf, 1, 0);
			}
		}
	}

	Ok(())
}

fn do_wakeup_with_lock(data: &mut GuardedData) -> Result<(ConnectionHandle, bool), Error> {
	let wakeup_scheduled = data.wakeup_scheduled;
	if !wakeup_scheduled {
		data.wakeup_scheduled = true;
	}
	Ok((data.wakeup_fd, wakeup_scheduled))
}

/// A handle that is associated with a particular connection and may be used for writing
/// to the socket.
///
/// This struct is passed into the callbacks specified by [`EventHandler::set_on_read`]
/// and [`EventHandler::set_on_client_read`] functions when data are ready.
/// It can then be used to write back to the associated connection.
/// It is also returned when a [`TcpStream`] is registered with the [`EventHandler`].
/// See [`EventHandler::set_on_read`] for examples on how to use it.
#[derive(Clone)]
pub struct WriteHandle {
	fd: ConnectionHandle,
	connection_id: u128,
	guarded_data: Arc<Mutex<GuardedData>>,
	fd_lock: Option<Arc<Mutex<StateInfo>>>,
}

impl WriteHandle {
	fn new(
		fd: ConnectionHandle,
		guarded_data: Arc<Mutex<GuardedData>>,
		connection_id: u128,
		fd_lock: Option<Arc<Mutex<StateInfo>>>,
	) -> Self {
		WriteHandle {
			fd,
			guarded_data,
			connection_id,
			fd_lock,
		}
	}

	/// Get the connection_id associated with this write handle.
	pub fn get_connection_id(&self) -> u128 {
		self.connection_id
	}

	/// Close the connection associated with this write handle.
	pub fn close(&self) -> Result<(), Error> {
		self.write(&[1], 0, 0, true)
	}

	/// Write the specifed data to the connection associated with this write handle.
	///
	/// * `data` - The data to write to this connection.
	/// * `offset` - The offset into this data buffer to start writing at.
	/// * `len` - The length of data to write.
	/// * `close` - Whether or not to close this connection after writing the data.
	pub fn write(&self, data: &[u8], offset: usize, len: usize, close: bool) -> Result<(), Error> {
		do_write(
			self.fd,
			data,
			offset,
			len,
			close,
			self.fd_lock.as_ref(),
			self.connection_id,
			&self.guarded_data,
		)
	}
}

#[derive(Debug, Clone)]
struct FdAction {
	fd: ConnectionHandle,
	atype: ActionType,
	seqno: u128,
}

impl FdAction {
	fn new(fd: ConnectionHandle, atype: ActionType, seqno: u128) -> FdAction {
		FdAction { fd, atype, seqno }
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
	fd: ConnectionHandle,
	seqno: u128,
}

impl HandlerEvent {
	fn new(fd: ConnectionHandle, etype: HandlerEventType, seqno: u128) -> Self {
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

#[derive(Debug, Clone)]
pub(crate) struct StreamWriteBuffer {
	fd: ConnectionHandle,
	write_buffer: WriteBuffer,
}

#[derive(Debug, Clone)]
pub(crate) struct WriteBuffer {
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
pub(crate) struct StateInfo {
	fd: ConnectionHandle,
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
	fn new(fd: ConnectionHandle, state: State, seqno: u128) -> Self {
		StateInfo {
			fd,
			state,
			seqno,
			write_buffer: LinkedList::new(),
		}
	}
}

struct GuardedData {
	fd_actions: Vec<FdAction>,
	wakeup_fd: ConnectionHandle,
	wakeup_rx: ConnectionHandle,
	wakeup_scheduled: bool,
	handler_events: Vec<HandlerEvent>,
	write_pending: Vec<ConnectionHandle>,
	selector: Option<ConnectionHandle>,
	stop: bool,
	write_queue: Vec<StreamWriteBuffer>,
}

/// EventHandler struct.
///
/// The EventHandler provides a simple interface for registering [`TcpStream`]'s and [`TcpListener`]'s such
/// that when data is available on any of the sockets, a user defined callback will be executed. Data
/// may also be written back to the sockets inband or out of band. The interface uses
/// [epoll](https://man7.org/linux/man-pages/man7/epoll.7.html) on linux,
/// [wepoll](https://github.com/piscisaureus/wepoll) on windows, and
/// [kqueues](https://www.freebsd.org/cgi/man.cgi?query=kqueue&sektion=2) on BSD variants, so perforamance
/// is optimized. In addition to using libraries that are optimized for their respective platforms, the
/// EventHandler preallocates all memory upon accepting a connection (once per file descriptor), so only
/// stack memory allocation is done when running with a heavy load. This allows for very good performance and
/// stable (low) memory usage. The EventHandler uses a little less than 100k RAM per connection, so 10,000
/// connections would require less than 1 GB of memory. When only a few hundred connections are needed, the
/// EventHandler uses around 1 mb of RAM so it would work fine on systems like a Raspberry Pi or other low
/// memory environments. This performance, coupled with Rust's memory safety and the ability to run user
/// defined rust programs allows for various applications.
/// # Examples
/// ```
/// // use required libraries
/// use std::io::Write;
/// use std::net::{TcpListener, TcpStream};
/// use std::sync::{Mutex, Arc};
/// use nioruntime_evh::EventHandler;
/// use nioruntime_util::Error;
///
/// fn main() -> Result<(), Error> {
///     // create a mutex to ensure functions are called
///     let x = Arc::new(Mutex::new(0));
///     let x_clone = x.clone();
///
///     // create a listener/stream with a port that is likely not used
///     let listener = TcpListener::bind("127.0.0.1:9991")?;
///     let mut stream = TcpStream::connect("127.0.0.1:9991")?;
///     // instantiate the EventHandler
///     let mut eh = EventHandler::new();
///
///     // set the on_read callback to simply echo back what is written to it
///     eh.set_on_read(|buf, len, wh| {
///         let _ = wh.write(buf, 0, len, false);
///         Ok(())
///     })?;
///
///     // don't do anything with the accept callback for now
///     eh.set_on_accept(|_| Ok(()))?;
///     // don't do anything with the close callback for now
///     eh.set_on_close(|_| Ok(()))?;
///     // assert that the client receives the echoed message back exactly
///     // as was sent
///     eh.set_on_client_read(move |buf, len, _wh| {
///         assert_eq!(len, 5);
///         assert_eq!(buf[0], 1);
///         assert_eq!(buf[1], 2);
///         assert_eq!(buf[2], 3);
///         assert_eq!(buf[3], 4);
///         assert_eq!(buf[4], 5);
///         let mut x = x.lock().unwrap();
///         (*x) += 1;
///         Ok(())
///     })?;
///
///     // start the event handler
///     eh.start()?;
///
///     // add the tcp listener
///     eh.add_tcp_listener(&listener)?;
///     // add the tcp stream and retreive the fd, seqno which are needed to write
///     // on this client through the EventHandler interface
///     let wh = eh.add_tcp_stream(&stream)?;
///     // send the message
///     wh.write(&[1, 2, 3, 4, 5], 5, 0, false)?;
///     // wait long enough to make sure the client got the message
///     std::thread::sleep(std::time::Duration::from_millis(100));
///     let x = x_clone.lock().unwrap();
///     // ensure that the client callback executed
///     assert_eq!((*x), 1);
///     Ok(())
/// }
/// ```
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
	/// Add a [`TcpStream`] to this EventHandler.
	///
	/// This function adds the specified [`TcpStream`] to this [`EventHandler`]. When data is read
	/// on this [`TcpStream`], the callback specified by [`EventHandler::set_on_client_read`] will be
	/// executed and the user can respond accordingly. Please note that
	/// the EventHanlder is only responsible for handling reads/writes/closes (when an error occurs,
	/// or the user specifies) on the stream. The calling function must ensure that the [`TcpStream`]
	/// stays in scope because [`TcpStream`]'s drop function will close the socket which will result
	/// in the [`EventHandler`] detecting the close of the socket and taking appropriate action.
	/// Also note that the caller is responsible for managing this resource. The [`EventHandler`] will
	/// never close a connection unless it is instructed by the caller through the [`WriteHandle`] interface
	/// or if an error occurs or the other side of the connection closes. Also, once the [`TcpStream`] is
	/// registered with the [`EventHandler`], all reads/writes to this stream should occur through the
	/// [`EventHandler`] interface. Calling functions in [`TcpStream`] like [`std::io::Read`] or
	/// [`std::io::Write`] will result in undefined behavior. If the stream closes for any reason, the
	/// callback specified by [`EventHandler::set_on_close`] will be executed to notify the user.
	/// This function returns the [`WriteHandle`]
	/// that may be used to write to the socket.
	/// See the example above on how to do that.
	/// This function will result in an error if an i/o error occurs while trying to configure the stream
	/// or the [`EventHandler`] has not been started by calling the [`EventHandler::start`] function.
	pub fn add_tcp_stream(&mut self, stream: &TcpStream) -> Result<WriteHandle, Error> {
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

		#[cfg(target_os = "windows")]
		{
			let fd = stream.as_raw_socket();

			let sockoptres = unsafe {
				ws2_32::setsockopt(
					fd,
					winapi::SOL_SOCKET,
					winapi::SO_SNDBUF,
					&WINSOCK_BUF_SIZE as *const _ as *const i8,
					std::mem::size_of_val(&WINSOCK_BUF_SIZE) as winapi::c_int,
				)
			};

			if sockoptres != 0 {
				error!("setsockopt resulted in error: {}", errno().to_string());
			}
		}

		#[cfg(any(
			target_os = "linux",
			target_os = "macos",
			dragonfly,
			freebsd,
			netbsd,
			openbsd
		))]
		let (fd, connection_id) = self.add_fd(stream.as_raw_fd(), ActionType::AddStream)?;
		#[cfg(target_os = "windows")]
		let (fd, connection_id) = self.add_socket(stream.as_raw_socket(), ActionType::AddStream)?;
		Ok(WriteHandle {
			fd,
			connection_id,
			guarded_data: self.data.clone(),
			fd_lock: None,
		})
	}

	/// Add a [`TcpListener`] to this EventHandler.
	///
	/// This function adds the specified [`TcpListener`] to this [`EventHandler`]. When a client connects to
	/// this [`TcpListener`], it is accepted by [`EventHandler`] and when any data is read on that accepted
	/// client's connection, the callback specified by [`EventHandler::set_on_read`] will be
	/// executed and the user can respond accordingly. The callback specified by [`EventHandler::set_on_accept`]
	/// will be executed when a new connection is accepted on this [`TcpListener`] and the callback specified
	/// by [`EventHandler::set_on_close`] will be executed when the connection closes. Please note that
	/// the EventHanlder is only responsible for accepting new connections on the [`TcpListener`]
	/// Closing the [`TcpListener`] or any accepted connection is the responsibility of the user.
	/// The calling function must
	/// ensure that the [`TcpListener`] stays in scope because [`TcpListener`]'s drop function will close
	/// the socket which will result in the [`EventHandler`] detecting the close of the listener and taking
	/// appropriate action. Also note that the caller is responsible for managing this resource. The
	/// [`EventHandler`] will never close a connection unless an error occurs. Also, once the [`TcpListener`] is
	/// registered with the [`EventHandler`], no additional function calls should be made directly to the
	/// [`TcpListener`] itself. Calling any functions in [`TcpListener`] will result in undefined behavior.
	/// This function will result in an error if an i/o error occurs while trying to configure the listener
	/// or the [`EventHandler`] has not been started by calling the [`EventHandler::start`] function.
	pub fn add_tcp_listener(&mut self, listener: &TcpListener) -> Result<(), Error> {
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
		self.add_fd(listener.as_raw_fd(), ActionType::AddListener)?;
		#[cfg(target_os = "windows")]
		self.add_socket(
			listener.as_raw_socket().try_into().unwrap_or(0),
			ActionType::AddListener,
		)?;
		Ok(())
	}

	/// This sets the on_read callback for this [`EventHandler`].
	///
	/// As described in [`EventHandler::add_tcp_listener`], this callback is executed when data is available
	/// on a connection that has been accepted by a [`TcpListener`] that was registered with this [`EventHandler`].
	/// # Examples
	/// ```
	/// use nioruntime_evh::EventHandler;
	/// use nioruntime_util::Error;
	///
	/// fn main() -> Result<(), Error> {
	///     let mut eh = EventHandler::new();
	///     // set the on_read callback to simply echo back what is written to it
	///     eh.set_on_read(|buf, len, wh| {
	///         let _ = wh.write(buf, 0, len, false);
	///         Ok(())
	///     })?;
	///     eh.set_on_accept(|_| Ok(()))?;
	///     eh.set_on_client_read(|_,_,_| Ok(()))?;
	///     eh.set_on_close(|_| Ok(()))?;
	///     Ok(())
	/// }
	/// ```
	pub fn set_on_read(&mut self, on_read: F) -> Result<(), Error> {
		let mut callbacks = self.callbacks.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		callbacks.on_read = Some(Box::pin(on_read));

		Ok(())
	}

	/// This sets the on_accept callback for this [`EventHandler`].
	///
	/// As described in [`EventHandler::add_tcp_listener`], this callback is executed when a new connection is
	/// accepted on a [`TcpListener`] that was registered with this [`EventHandler`].
	///
	/// # Examples
	/// ```
	/// use nioruntime_evh::EventHandler;
	/// use nioruntime_util::Error;
	/// use log::*;
	///
	/// // set log level to info
	/// info!();
	///
	/// fn main() -> Result<(), Error> {
	///     let mut eh = EventHandler::new();
	///     // print out a message when a new connection is accepted
	///     eh.set_on_accept(|connection_id| {
	///         info!("accepted connection with id = {}", connection_id);
	///         Ok(())
	///     })?;
	///     eh.set_on_read(|_,_,_|  Ok(()))?;
	///     eh.set_on_client_read(|_,_,_| Ok(()))?;
	///     eh.set_on_close(|_| Ok(()))?;
	///     Ok(())
	/// }   
	/// ```
	pub fn set_on_accept(&mut self, on_accept: G) -> Result<(), Error> {
		let mut callbacks = self.callbacks.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		callbacks.on_accept = Some(Box::pin(on_accept));

		Ok(())
	}

	/// This sets the on_close callback for this [`EventHandler`].
	///
	/// As described in [`EventHandler::add_tcp_listener`], this callback is executed when a connection is
	/// closed on a [`TcpListener`] that was registered with this [`EventHandler`]. This may happen due to
	/// an error, if the other side disconnects and EOF is reached or if the user closes this connection
	/// via the [`WriteHandle`].
	///
	/// # Examples
	/// ```
	/// use nioruntime_evh::EventHandler;
	/// use nioruntime_util::Error;
	/// use log::*;
	///
	/// // set log level to info
	/// info!();
	///
	/// fn main() -> Result<(), Error> {
	///     let mut eh = EventHandler::new();
	///     // print out a message when a connection is closed
	///     eh.set_on_close(|connection_id| {
	///         info!("closed connection with id = {}", connection_id);
	///         Ok(())
	///     })?;
	///     eh.set_on_read(|_,_,_|  Ok(()))?;
	///     eh.set_on_client_read(|_,_,_| Ok(()))?;
	///     eh.set_on_accept(|_| Ok(()))?;
	///     Ok(())
	/// }
	/// ```
	pub fn set_on_close(&mut self, on_close: H) -> Result<(), Error> {
		let mut callbacks = self.callbacks.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		callbacks.on_close = Some(Box::pin(on_close));

		Ok(())
	}

	/// This sets the on_client_read callback for this [`EventHandler`].
	///
	/// As described in [`EventHandler::add_tcp_stream`], this callback is executed when a connection
	/// has data available for reading on a [`TcpStream`] that was registered with this [`EventHandler`].
	/// The only difference between this callback and [`EventHandler::set_on_read`] is that this one is
	/// called for [`TcpStream`]'s that were registed with the [`EventHandler`] and [`EventHandler::set_on_read`]
	/// is used for connections that were accepted with a registered [`TcpListener`].
	///
	/// # Examples
	/// ```
	/// use nioruntime_evh::EventHandler;
	/// use nioruntime_util::Error;
	/// use log::*;
	///
	/// // set log level to info
	/// info!();
	///
	/// fn main() -> Result<(), Error> {
	///     let mut eh = EventHandler::new();
	///     // echo back the message
	///     eh.set_on_client_read(|buf, len, wh| {
	///         let _ = wh.write(buf, 0, len, false);
	///         Ok(())
	///     })?;
	///     eh.set_on_close(|_| Ok(()))?;
	///     eh.set_on_read(|_,_,_|  Ok(()))?;
	///     eh.set_on_accept(|_| Ok(()))?;
	///     Ok(())
	/// }
	/// ```
	pub fn set_on_client_read(&mut self, on_client_read: K) -> Result<(), Error> {
		let mut callbacks = self.callbacks.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!("Poison Error: {}", e)).into();
			error
		})?;

		callbacks.on_client_read = Some(Box::pin(on_client_read));

		Ok(())
	}

	/// Create a new instance of the [`EventHandler`]. Note that all callbacks must be registered
	/// and [`EventHandler::start`] must be called before handleing events. See the example in the
	/// [`EventHandler`] section of the documentation for a full details.
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
						info!("Error creating socket_pipe on windows, {}", e.to_string());
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
			wakeup_fd: tx as ConnectionHandle,
			wakeup_rx: rx as ConnectionHandle,
			wakeup_scheduled: false,
			handler_events: vec![],
			write_pending: vec![],
			selector: None,
			stop: false,
			write_queue: vec![],
		};
		let guarded_data = Arc::new(Mutex::new(guarded_data));

		EventHandler {
			data: guarded_data,
			callbacks,
			_pipe_listener,
			_pipe_stream,
		}
	}

	/// Start the event handler.
	pub fn start(&mut self) -> Result<(), Error> {
		self.do_start()
	}

	/// Stop the event handler and free any internal resources associated with it. Note: this
	/// does not close any registered sockets. That is the responsibility of the user.
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
				ws2_32::send(guarded_data.wakeup_fd, buf, 1, 0);
			}
		}

		Ok(())
	}

	#[cfg(target_os = "linux")]
	fn do_start(&mut self) -> Result<(), Error> {
		// create poll fd
		let selector = epoll_create1(EpollCreateFlags::empty())?;
		self.start_generic(selector)?;
		Ok(())
	}

	#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
	fn do_start(&mut self) -> Result<(), Error> {
		// create the kqueue
		let selector = unsafe { kqueue() };
		self.start_generic(selector)?;
		Ok(())
	}

	#[cfg(target_os = "windows")]
	fn do_start(&mut self) -> Result<(), Error> {
		self.start_generic(0)?;
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

	#[cfg(target_os = "windows")]
	fn add_socket(
		&mut self,
		socket: ConnectionHandle,
		atype: ActionType,
	) -> Result<(ConnectionHandle, u128), Error> {
		let fd = socket;
		let (fd, seqno) = self.add_fd(fd, atype)?;
		Ok((fd, seqno))
	}

	fn add_fd(
		&mut self,
		fd: ConnectionHandle,
		atype: ActionType,
	) -> Result<(ConnectionHandle, u128), Error> {
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
		let seqno: u128 = rand::random();
		fd_actions.push(FdAction::new(fd, atype, seqno));
		Ok((fd.into(), seqno))
	}

	#[cfg(target_os = "windows")]
	fn socket_pipe(fds: *mut i32) -> Result<(TcpListener, TcpStream), Error> {
		let port = portpicker::pick_unused_port().unwrap_or(9999);
		let listener = TcpListener::bind(format!("127.0.0.1:{}", port))?;
		let stream = TcpStream::connect(format!("127.0.0.1:{}", port))?;
		let res = unsafe {
			accept(
				listener.as_raw_socket(),
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

	fn start_generic(&mut self, selector: ConnectionHandle) -> Result<(), Error> {
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
				info!("win_selector is null. Cannot start");
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
						info!("poll loop done");
					}
					Err(e) => {
						info!("FATAL: Unexpected error in poll loop: {}", e.to_string());
					}
				}
			}
		});

		Ok(())
	}

	fn process_handler_events(
		handler_events: Vec<HandlerEvent>,
		write_pending: Vec<ConnectionHandle>,
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
			Err(e) => info!("Error obtaining global lock: {}", e),
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
										info!(
											"on close handler generated error: {}",
											e.to_string()
										);
									}
								}
							})?;
						}
						Err(e) => {
							info!(
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
		_selector: ConnectionHandle,
		win_selector: *mut c_void,
		input_events: Vec<GenericEvent>,
		output_events: &mut Vec<GenericEvent>,
		filter_set: &mut HashSet<ConnectionHandle>,
	) -> Result<ConnectionHandle, Error> {
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
				let data = epoll_data_t {
					fd: evt.fd.try_into().unwrap_or(0),
				};
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
				let data = epoll_data_t {
					fd: evt.fd.try_into().unwrap_or(0),
				};
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
					info!(
						"epoll_ctl (write) resulted in an unexpected error: {}, fd={}, op={}, epoll_ctl_add={}",
						errno().to_string(), evt.fd, op, EPOLL_CTL_ADD,
					);
				}
			} else if evt.etype == GenericEventType::DelRead {
				filter_set.remove(&evt.fd);
				let data = epoll_data_t {
					fd: evt.fd.try_into().unwrap_or(0),
				};
				let mut event = epoll_event {
					events: 0, // not used for del
					data,
				};

				let res = unsafe {
					epoll_ctl(
						win_selector,
						EPOLL_CTL_DEL,
						evt.fd.try_into().unwrap_or(0),
						&mut event,
					)
				};

				if res != 0 {
					info!(
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
						unsafe { events[i as usize].data.fd } as ConnectionHandle,
						GenericEventType::AddWriteET,
					));
				}
				if !(events[i as usize].events & EPOLLIN == 0) {
					ret_count_adjusted += 1;
					output_events.push(GenericEvent::new(
						unsafe { events[i as usize].data.fd } as ConnectionHandle,
						GenericEventType::AddReadET,
					));
				}
				if events[i as usize].events & (EPOLLIN | EPOLLOUT) == 0 {
					let fd = unsafe { events[i as usize].data.fd };
					let data = epoll_data_t {
						fd: fd.try_into().unwrap_or(0),
					};
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
						info!(
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
		epollfd: ConnectionHandle,
		_win_selector: *mut c_void,
		input_events: Vec<GenericEvent>,
		output_events: &mut Vec<GenericEvent>,
		filter_set: &mut HashSet<ConnectionHandle>,
	) -> Result<ConnectionHandle, Error> {
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

				let mut event = EpollEvent::new(interest, evt.fd.try_into().unwrap_or(0));
				let res = epoll_ctl(epollfd, op, evt.fd, &mut event);
				match res {
					Ok(_) => {}
					Err(e) => info!("Error epoll_ctl1: {}, fd={}, op={:?}", e, fd, op),
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

				let mut event = EpollEvent::new(interest, evt.fd.try_into().unwrap_or(0));
				let res = epoll_ctl(epollfd, op, evt.fd, &mut event);
				match res {
					Ok(_) => {}
					Err(e) => info!("Error epoll_ctl2: {}, fd={}, op={:?}", e, fd, op),
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

				let mut event = EpollEvent::new(interest, evt.fd.try_into().unwrap_or(0));
				let res = epoll_ctl(epollfd, op, evt.fd, &mut event);
				match res {
					Ok(_) => {}
					Err(e) => info!("Error epoll_ctl3: {}, fd={}, op={:?}", e, fd, op),
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
								events[i].data() as ConnectionHandle,
								GenericEventType::AddWriteET,
							));
						}
						if !(events[i].events() & EpollFlags::EPOLLIN).is_empty() {
							ret_count_adjusted += 1;
							output_events.push(GenericEvent::new(
								events[i].data() as ConnectionHandle,
								GenericEventType::AddReadET,
							));
						}
					}
				}
			}
			Err(e) => {
				info!("Error with epoll wait = {}", e.to_string());
			}
		}

		Ok(ret_count_adjusted)
	}

	#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
	fn get_events(
		queue: ConnectionHandle,
		_win_selector: *mut c_void,
		input_events: Vec<GenericEvent>,
		output_events: &mut Vec<GenericEvent>,
		_filter_set: &HashSet<ConnectionHandle>,
	) -> Result<ConnectionHandle, Error> {
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
				&duration_to_timespec(std::time::Duration::from_millis(500)),
			)
		};

		// This error appears to happen occasionally when thousands of
		// connections per second are connecting/disconnecting.
		// it appears harmless as all connections are processed correctly.
		// So for the time being, we will comment this out so it doesn't
		// pollute the logs.
		if ret_count < 0 {
			info!("Error in kevent: kevs={:?}, error={}", kevs, errno());
		}

		let mut ret_count_adjusted = 0;
		for i in 0..ret_count {
			let kev = ret_kevs[i as usize];
			if !kev.flags.contains(EventFlag::EV_DELETE) {
				if kev.filter == EVFILT_WRITE {
					ret_count_adjusted += 1;
					output_events.push(GenericEvent::new(
						kev.ident.try_into().unwrap_or(0),
						GenericEventType::AddWriteET,
					));
				}
				if kev.filter == EVFILT_READ {
					ret_count_adjusted += 1;
					output_events.push(GenericEvent::new(
						kev.ident.try_into().unwrap_or(0),
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
		selector: ConnectionHandle,
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
			let write_queue;
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
				write_queue = guarded_data.write_queue.clone();
				to_process = guarded_data.fd_actions.clone();
				handler_events = guarded_data.handler_events.clone();
				write_pending = guarded_data.write_pending.clone();
				guarded_data.write_queue.clear();
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
							info!("Error closing selector: {}", errno().to_string());
						}
						let res = unsafe { close(guarded_data.wakeup_fd) };
						if res != 0 {
							info!("Error closing selector: {}", errno().to_string());
						}
					}
					#[cfg(target_os = "windows")]
					{
						let res = unsafe { epoll_close(win_selector) };
						if res != 0 {
							info!("Error closing win_selector: {}", errno().to_string());
						}
						let res = unsafe { ws2_32::closesocket(guarded_data.wakeup_fd) };
						if res != 0 {
							info!("Error closing selector: {}", errno().to_string());
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

			for buffer in write_queue {
				if fd_locks.len() <= buffer.fd as usize {
					Self::check_and_set(
						fd_locks,
						buffer.fd as usize,
						Arc::new(Mutex::new(StateInfo::new(
							buffer.fd,
							State::Normal,
							buffer.write_buffer.connection_seqno,
						))),
					);
				}
				do_write(
					buffer.fd,
					&buffer.write_buffer.buffer,
					buffer.write_buffer.offset.into(),
					buffer.write_buffer.len.into(),
					buffer.write_buffer.close,
					Some(&fd_locks[buffer.fd as usize]),
					buffer.write_buffer.connection_seqno,
					guarded_data,
				)?;
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

						if on_read_locks.len() <= fd as usize {
							Self::check_and_set(
								on_read_locks,
								fd as usize,
								Arc::new(Mutex::new(false)),
							);
						}

						if fd_locks.len() <= fd as usize {
							Self::check_and_set(
								fd_locks,
								fd as usize,
								Arc::new(Mutex::new(StateInfo::new(
									fd.try_into().unwrap_or(0),
									State::Normal,
									proc.seqno,
								))),
							);
						}
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
				if event.etype == GenericEventType::AddWriteET {
					let res = Self::process_event_write(
						event.fd as ConnectionHandle,
						&thread_pool,
						guarded_data,
						&global_lock,
						fd_locks,
					);
					match res {
						Ok(_) => {}
						Err(e) => {
							info!("Unexpected error in poll loop: {}", e.to_string());
						}
					}
				}
				if event.etype == GenericEventType::AddReadET
					|| event.etype == GenericEventType::AddReadLT
				{
					let res = Self::process_event_read(
						event.fd as ConnectionHandle,
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
						&mut use_on_client_read,
					);

					match res {
						Ok(_) => {}
						Err(e) => {
							info!("Unexpected error in poll loop: {}", e.to_string());
						}
					}
				}
			}
		}

		Ok(())
	}

	fn write_loop(
		fd: ConnectionHandle,
		statefd: ConnectionHandle,
		write_buffer: &mut WriteBuffer,
	) -> Result<u16, Error> {
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
				let buf: *mut c_void = &mut write_buffer.buffer[(write_buffer.offset as usize)
					..(write_buffer.offset as usize + write_buffer.len as usize)]
					as *mut _ as *mut c_void;

				unsafe { write(fd, buf, write_buffer.len.into()) }
			};
			#[cfg(target_os = "windows")]
			let len = {
				let buf: *mut i8 = &mut write_buffer.buffer
					[(write_buffer.offset as usize)..(write_buffer.len as usize)]
					as *mut _ as *mut i8;

				unsafe { ws2_32::send(fd, buf, (write_buffer.len - write_buffer.offset).into(), 0) }
			};
			if len >= 0 {
				if len == (write_buffer.len as isize) {
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
		fd: ConnectionHandle,
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
						let res = unsafe { ws2_32::closesocket(state.fd) };
						if res == 0 {
							state.state = State::Closed;
						} else {
							let e = errno();
							info!("error closing socket: {}", e.to_string());
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
				info!(
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
		fd: ConnectionHandle,
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
					info!("write error: {}", e);
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
		fd: ConnectionHandle,
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
					info!(
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
						Err(e) => info!("Unexpected error obtaining write lock: {}", e),
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
										info!(
											"unexpected error in process_event_write: {}",
											e.to_string()
										);
									}
								}
								#[cfg(unix)]
								match res {
									Ok(_) => {}
									Err(e) => {
										info!(
											"unexpected error in process_event_write: {}",
											e.to_string()
										);
									}
								}
							}
							Err(e) => {
								info!(
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
								info!("Error pushing handler event: {}", e);
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

	fn ensure_allocations(
		fd: ConnectionHandle,
		read_fd_type: &mut Vec<FdType>,
		fd_locks: &mut Vec<Arc<Mutex<StateInfo>>>,
		on_read_locks: &mut Vec<Arc<Mutex<bool>>>,
		use_on_client_read: &mut Vec<bool>,
		seqno: u128,
	) -> Result<(), Error> {
		let len = read_fd_type.len();
		if fd as usize >= len {
			for _ in len..(fd + 100) as usize + 1 {
				read_fd_type.push(FdType::Unknown);
			}
		}

		if on_read_locks.len() <= fd as usize {
			Self::check_and_set(
				on_read_locks,
				(fd + 100) as usize,
				Arc::new(Mutex::new(false)),
			);
		}

		if fd_locks.len() <= fd as usize {
			Self::check_and_set(
				fd_locks,
				(fd + 100) as usize,
				Arc::new(Mutex::new(StateInfo::new(fd, State::Normal, seqno))),
			);
		}

		if use_on_client_read.len() <= fd as usize {
			Self::check_and_set(use_on_client_read, (fd + 100) as usize, false);
		}

		{
			let state = fd_locks[fd as usize].lock();
			match state {
				Ok(mut state) => {
					*state = StateInfo::new(fd, State::Normal, seqno);
				}
				Err(e) => {
					info!("Error getting seqno: {}", e.to_string());
					return Err(ErrorKind::InternalError(
						"unexpected error obtaining seqno".to_string(),
					)
					.into());
				}
			}
		}
		Ok(())
	}

	fn process_event_read(
		fd: ConnectionHandle,
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
		use_client_on_read: &mut Vec<bool>,
	) -> Result<(), Error> {
		let fd_type = &read_fd_type[fd as usize];
		match fd_type {
			FdType::Listener => {
				let lock = global_lock.write();
				match lock {
					Ok(_) => {}
					Err(e) => info!("Unexpected error obtaining read lock, {}", e),
				}
				#[cfg(unix)]
				let res = unsafe {
					accept(
						fd,
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
						fd,
						&mut winapi::ws2def::SOCKADDR {
							..std::mem::zeroed()
						},
						&mut (std::mem::size_of::<winapi::ws2def::SOCKADDR>() as u32)
							.try_into()
							.unwrap_or(0),
					)
				};
				if res > 0 {
					Self::ensure_allocations(
						res,
						read_fd_type,
						fd_locks,
						on_read_locks,
						use_client_on_read,
						seqno,
					)?;

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
						let ioctl_res =
							unsafe { ws2_32::ioctlsocket(res, fionbio as c_int, &mut 1) };

						if ioctl_res != 0 {
							info!("complete fion with error: {}", errno().to_string());
						}

						let sockoptres = unsafe {
							ws2_32::setsockopt(
								res,
								winapi::SOL_SOCKET,
								winapi::SO_SNDBUF,
								&WINSOCK_BUF_SIZE as *const _ as *const i8,
								std::mem::size_of_val(&WINSOCK_BUF_SIZE) as winapi::c_int,
							)
						};

						if sockoptres != 0 {
							info!("setsockopt resulted in error: {}", errno().to_string());
						}
					}

					let accept_res = Self::process_accept_result(fd, res, &guarded_data, fd_locks);
					match accept_res {
						Ok(_) => {}
						Err(e) => {
							info!("process_accept_result resulted in: {}", e.to_string())
						}
					}
					let accept_res = (on_accept)(seqno as u128);
					match accept_res {
						Ok(_) => {}
						Err(e) => info!("on_accept callback resulted in: {}", e.to_string()),
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
							info!(
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
							Err(e) => info!("Unexpected error obtaining read lock: {}", e),
						}
						let mut buf = [0u8; BUFFER_SIZE];
						loop {
							let on_read_lock = on_read_locks[fd as usize].lock();
							match on_read_lock {
								Ok(_) => {}
								Err(e) => {
									info!("unexpected error obtaining on_read_lock: {}", e);
								}
							}
							let (seqno, len) = {
								let fd_lock = fd_lock.lock();
								let seqno = match fd_lock {
									Ok(ref state_info) => (*state_info).seqno,
									Err(e) => {
										info!(
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
											fd,
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
								{
									break;
								}
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
								true,
								seqno,
							);
							match res {
								Ok(_) => {}
								Err(e) => {
									info!("Error pushing handler event: {}", e);
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
				info!("unexpected fd_type (unknown) for fd: {}", fd);
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
					unsafe { ws2_32::recv(fd, cbuf, BUFFER_SIZE.try_into().unwrap_or(0), 0) }
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
		fd: ConnectionHandle,
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
				Some(fd_locks[fd as usize].clone()),
			);

			let result = match use_on_client_read {
				true => (on_client_read)(&buf, len, wh),
				false => (on_read)(&buf, len, wh),
			};

			match result {
				Ok(_) => {}
				Err(e) => {
					info!("Client callback resulted in error: {}", e.to_string());
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
		fd: ConnectionHandle,
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
		_acceptor: ConnectionHandle,
		nfd: ConnectionHandle,
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

	fn process_accept_err(_acceptor: ConnectionHandle, error: String) -> Result<(), Error> {
		info!("error on acceptor: {}", error);
		Ok(())
	}

	fn push_handler_event_with_fd_lock(
		fd: ConnectionHandle,
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
		fd: ConnectionHandle,
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
					info!(
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
		fd: ConnectionHandle,
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
					info!("Unexpected handler error: {}", e.to_string());
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
						ws2_32::send(wakeup_fd, buf, 1, 0);
					}
				}
			}
		}
		Ok(())
	}
}

#[test]
fn test_echo() -> Result<(), Error> {
	use std::net::TcpListener;
	use std::net::TcpStream;

	let x = Arc::new(Mutex::new(0));
	let x_clone = x.clone();

	let listener = TcpListener::bind("127.0.0.1:9981")?;
	let stream = TcpStream::connect("127.0.0.1:9981")?;
	let mut eh = EventHandler::new();

	// echo
	eh.set_on_read(|buf, len, wh| {
		info!("server received: {} bytes", len);
		let _ = wh.write(buf, 0, len, false);
		Ok(())
	})?;

	eh.set_on_accept(|_| Ok(()))?;
	eh.set_on_close(|_| Ok(()))?;
	let client_buf = Arc::new(Mutex::new(vec![]));
	eh.set_on_client_read(move |buf, len, _wh| {
		info!("client received: {} bytes", len);
		let mut client_buf = client_buf.lock().unwrap();
		for i in 0..len {
			client_buf.push(buf[i]);
		}
		let len = client_buf.len();
		if len == 5 {
			assert_eq!(len, 5);
			assert_eq!(client_buf[0], 1);
			assert_eq!(client_buf[1], 2);
			assert_eq!(client_buf[2], 3);
			assert_eq!(client_buf[3], 4);
			assert_eq!(client_buf[4], 5);
			let mut x = x.lock().unwrap();
			(*x) += 5;
		}
		Ok(())
	})?;

	eh.start()?;

	eh.add_tcp_listener(&listener)?;
	let wh = eh.add_tcp_stream(&stream)?;
	wh.write(&[1, 2, 3, 4, 5], 0, 5, false)?;
	loop {
		{
			let x = x_clone.lock().unwrap();
			if *x != 0 {
				assert_eq!(*x, 5);
				break;
			}
		}
		std::thread::sleep(std::time::Duration::from_millis(10));
	}
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

	let read_buf = Arc::new(Mutex::new(vec![]));
	eh.set_on_read(move |buf, len, wh| {
		let mut read_buf = read_buf.lock().unwrap();
		for i in 0..len {
			read_buf.push(buf[i]);
		}

		if read_buf.len() < 5 {
			return Ok(());
		}

		let len = read_buf.len();

		for i in 0..len {
			if read_buf[i] == 7 {
				wh.close()?;
				return Ok(());
			}
		}
		for i in 0..len {
			if read_buf[i] == 8 {
				wh.write(&[0], 0, 1, true)?;
				return Ok(());
			}
		}
		wh.write(&[1], 0, 1, false)?;
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
	assert_eq!(len, 1);
	assert_eq!(buf[0], 1);

	stream.write(&[8, 8, 8, 8, 8])?;
	let len = stream.read(&mut buf)?;
	assert_eq!(len, 1);
	assert_eq!(buf[0], 0);
	let len = stream.read(&mut buf)?;
	assert_eq!(len, 0); // means connection closed

	let mut stream2 = TcpStream::connect("127.0.0.1:9982")?;
	stream2.write(&[7, 7, 7, 7, 7])?;
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
		info!("client_read={:?}", &buf[0..len]);
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
	eh.set_on_read(move |_, _, _| {
		let mut x = xclone.lock().unwrap();
		*x += 1;
		Ok(())
	})?;

	eh.set_on_accept(|_| Ok(()))?;
	eh.set_on_close(|_| Ok(()))?;
	eh.set_on_client_read(move |_, _, _| Ok(()))?;

	eh.start()?;
	eh.add_tcp_listener(&listener)?;
	eh.add_tcp_stream(&stream)?;
	stream.write(&[1])?;
	loop {
		{
			let x = x.lock().unwrap();
			if *x == 1 {
				break;
			}
		}
		std::thread::sleep(std::time::Duration::from_millis(10));
	}
	eh.stop()?;
	std::thread::sleep(std::time::Duration::from_millis(3000));
	stream.write(&[1])?;
	std::thread::sleep(std::time::Duration::from_millis(3000));
	let x = x.lock().unwrap();
	assert_eq!(*x, 1);
	Ok(())
}

#[test]
fn test_large_messages() -> Result<(), Error> {
	use std::net::TcpListener;
	use std::net::TcpStream;

	let listener = TcpListener::bind("127.0.0.1:9933")?;
	let stream = TcpStream::connect("127.0.0.1:9933")?;
	let mut eh = EventHandler::new();

	eh.set_on_read(move |buf, len, wh| {
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

	let mut msgbuf = vec![];
	for i in 0..3_000_000 {
		msgbuf.push((i % 123) as u8);
	}
	msgbuf.push(128);
	let cloned_msgbuf = msgbuf.clone();

	eh.set_on_accept(|_| Ok(()))?;
	eh.set_on_close(|_| Ok(()))?;

	let client_accumulator = Arc::new(Mutex::new(vec![]));
	let complete = Arc::new(Mutex::new(false));
	let complete_clone = complete.clone();

	eh.set_on_client_read(move |buf, len, _wh| {
		let mut client_accumulator = client_accumulator.lock().unwrap();
		for i in 0..len {
			client_accumulator.push(buf[i]);
			if buf[i] == 128 {
				assert_eq!(client_accumulator.len(), cloned_msgbuf.len());
				assert_eq!(*client_accumulator, cloned_msgbuf);
				let mut complete = complete_clone.lock().unwrap();
				*complete = true;
			}
		}
		Ok(())
	})?;

	eh.start()?;
	eh.add_tcp_listener(&listener)?;
	let wh = eh.add_tcp_stream(&stream)?;
	wh.write(&msgbuf, 0, msgbuf.len(), false)?;
	loop {
		{
			let complete = complete.lock().unwrap();
			if *complete {
				break;
			}
		}
		std::thread::sleep(std::time::Duration::from_millis(10));
	}
	Ok(())
}
