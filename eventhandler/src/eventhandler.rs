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
// limitations under the License

use errno::errno;
use errno::Errno;
use libc::{accept, c_int, c_void, EAGAIN};
use nioruntime_log::*;
use nioruntime_util::{Error, ErrorKind};
use rand::Rng;
use rustls::{Connection, ServerConfig, ServerConnection};
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::convert::TryInto;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::pin::Pin;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::SyncSender;
use std::sync::RwLockWriteGuard;
use std::sync::{Arc, RwLock};
use std::thread::spawn;

pub type OnPanic = fn() -> Result<(), Error>;

// linux deps
#[cfg(target_os = "linux")]
use nix::sys::epoll::{
	epoll_create1, epoll_ctl, epoll_wait, EpollCreateFlags, EpollEvent, EpollFlags, EpollOp,
};

// macos/bsd deps
#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
use crate::duration_to_timespec;
#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
use kqueue_sys::EventFilter::{self, EVFILT_READ, EVFILT_WRITE};
#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
use kqueue_sys::{kevent, kqueue, EventFlag, FilterFlag};
#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
use libc::uintptr_t;

// unix deps
#[cfg(unix)]
use libc::{close, fcntl, pipe, read, write};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

debug!();

const MAIN_LOG: &str = "mainlog";
const BUFFER_SIZE: usize = 1024;
const TLS_CHUNKS: usize = 32768;
const MAX_EVENTS: i32 = 100;
#[cfg(target_os = "windows")]
const WINSOCK_BUF_SIZE: winapi::c_int = 100_000_000;

#[cfg(unix)]
type ConnectionHandle = i32;
#[cfg(target_os = "windows")]
type ConnectionHandle = u64;

fn load_certs(filename: &str) -> Vec<rustls::Certificate> {
	let certfile = File::open(filename).expect("cannot open certificate file");
	let mut reader = BufReader::new(certfile);
	rustls_pemfile::certs(&mut reader)
		.unwrap()
		.iter()
		.map(|v| rustls::Certificate(v.clone()))
		.collect()
}

fn load_private_key(filename: &str) -> rustls::PrivateKey {
	let keyfile = File::open(filename).expect("cannot open private key file");
	let mut reader = BufReader::new(keyfile);

	loop {
		match rustls_pemfile::read_one(&mut reader).expect("cannot parse private key .pem file") {
			Some(rustls_pemfile::Item::RSAKey(key)) => return rustls::PrivateKey(key),
			Some(rustls_pemfile::Item::PKCS8Key(key)) => return rustls::PrivateKey(key),
			None => break,
			_ => {}
		}
	}

	panic!(
		"no keys found in {:?} (encrypted keys not supported)",
		filename
	);
}

#[derive(Eq, PartialEq, Debug)]
pub enum State {
	Init,
	HeadersChunked,
	HeadersClose,
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
	guarded_data: Arc<RwLock<GuardedData>>,
	global_lock: Arc<RwLock<bool>>,
	pub callback_state: Arc<RwLock<State>>,
	tls_conn: Option<Arc<RwLock<ServerConnection>>>,
}

impl WriteHandle {
	fn new(
		fd: ConnectionHandle,
		guarded_data: Arc<RwLock<GuardedData>>,
		connection_id: u128,
		global_lock: Arc<RwLock<bool>>,
		tls_conn: Option<Arc<RwLock<ServerConnection>>>,
	) -> Self {
		let callback_state = {
			let guarded_data = guarded_data.write().unwrap();
			guarded_data.callback_state.clone()
		};
		WriteHandle {
			fd,
			guarded_data,
			connection_id,
			global_lock,
			callback_state,
			tls_conn,
		}
	}

	/// Get the connection_id associated with this write handle.
	pub fn get_connection_id(&self) -> u128 {
		self.connection_id
	}

	/// Close the connection associated with this write handle.
	pub fn close(&self) -> Result<(), Error> {
		let buf = [0u8; BUFFER_SIZE];
		let mut guarded_data = nioruntime_util::lockw!(self.guarded_data);
		let wbuffer = WriteBuffer {
			buffer: buf.clone(),
			offset: 0,
			len: 0,
			close: true,
			connection_id: self.connection_id,
		};
		guarded_data.write_queue.push(wbuffer);
		guarded_data.wakeup()?;

		Ok(())
	}

	pub fn async_recheck(&self) -> Result<(), Error> {
		let mut guarded_data = nioruntime_util::lockw!(self.guarded_data);
		let conn = ConnectionInfo {
			handle: self.fd,
			connection_id: self.connection_id,
			ctype: ConnectionType::Inbound,
			sender: None,
			tls_conn: None,
		};
		guarded_data.aconns.push(conn);
		guarded_data.wakeup()?;

		Ok(())
	}

	/// Write the specifed data to the connection associated with this write handle.
	///
	/// * `data` - The data to write to this connection.
	pub fn write(&self, data: &[u8]) -> Result<(), Error> {
		match &self.tls_conn {
			Some(tls_conn) => {
				let mut wbuf = vec![];
				{
					let mut tls_conn = nioruntime_util::lockw!(tls_conn);
					let mut start = 0;
					loop {
						let mut end = data.len();
						if end - start > TLS_CHUNKS {
							end = start + TLS_CHUNKS;
						}
						tls_conn.writer().write_all(&data[start..end])?;
						tls_conn.write_tls(&mut wbuf)?;
						if end == data.len() {
							break;
						}
						start += TLS_CHUNKS;
					}
				}
				self.do_write(&wbuf)
			}
			None => self.do_write(data),
		}
	}

	fn do_write(&self, data: &[u8]) -> Result<(), Error> {
		let len = data.len();
		if len == 0 {
			// nothing to write
			return Ok(());
		}
		let mut buf = [0u8; BUFFER_SIZE];
		let mut guarded_data = nioruntime_util::lockw!(self.guarded_data);
		let mut start = 0;
		let len = data.len();
		let mut end = if len > BUFFER_SIZE { BUFFER_SIZE } else { len };
		let mut rem = len;
		loop {
			let len = if rem < BUFFER_SIZE { rem } else { BUFFER_SIZE };
			buf[..len].clone_from_slice(&data[start..end]);
			let wbuffer = WriteBuffer {
				buffer: buf.clone(),
				offset: 0,
				len: len.try_into().unwrap_or(0),
				close: false,
				connection_id: self.connection_id,
			};
			guarded_data.write_queue.push(wbuffer);
			if rem <= BUFFER_SIZE {
				break;
			}
			rem -= BUFFER_SIZE;
			start += BUFFER_SIZE;
			end += BUFFER_SIZE;
			if end > data.len() {
				end = data.len();
			}
		}
		guarded_data.wakeup()?;

		Ok(())
	}
}

#[derive(Clone)]
pub struct TlsConfig {
	pub private_key_file: String,
	pub certificates_file: String,
}

impl TlsConfig {
	pub fn new(private_key_file: String, certificates_file: String) -> Self {
		TlsConfig {
			private_key_file,
			certificates_file,
		}
	}
}

#[derive(Clone)]
pub struct EventHandlerConfig {
	/// Number of threads for handling read/write operations. The default value is 6.
	pub thread_count: usize,
	/// The optional TLS config. If not specified, the server will run in non-ssl
	/// mode.
	pub tls_config: Option<TlsConfig>,
}

impl Default for EventHandlerConfig {
	fn default() -> EventHandlerConfig {
		EventHandlerConfig {
			thread_count: 6,
			tls_config: None,
		}
	}
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
/// use std::sync::{RwLock, Arc};
/// use nioruntime_evh::EventHandler;
/// use nioruntime_util::Error;
/// use nioruntime_evh::EventHandlerConfig;
///
/// nioruntime_log::info!();
///
/// fn main() -> Result<(), Error> {
///     // create a mutex to ensure functions are called
///     let x = Arc::new(RwLock::new(0));
///     let x_clone = x.clone();
///
///     // create a listener/stream with a port that is likely not used
///     let listener = TcpListener::bind("127.0.0.1:9991")?;
///     let mut stream = TcpStream::connect("127.0.0.1:9991")?;
///     // instantiate the EventHandler
///     let mut eh = EventHandler::new(EventHandlerConfig::default());
///
///     // set the on_read callback to simply echo back what is written to it
///     eh.set_on_read(|buf, len, wh| {
///         nioruntime_log::info!("on read");
///         let _ = wh.write(&buf[0..len]);
///         Ok(())
///     })?;
///
///     // don't do anything with the accept callback for now
///     eh.set_on_accept(|_,_| Ok(()))?;
///     // don't do anything with the close callback for now
///     eh.set_on_close(|_| Ok(()))?;
///     // assert that the client receives the echoed message back exactly
///     // as was sent
///     eh.set_on_client_read(move |buf, len, _wh| {
///         nioruntime_log::info!("on client read");
///         assert_eq!(len, 5);
///         assert_eq!(buf[0], 1);
///         assert_eq!(buf[1], 2);
///         assert_eq!(buf[2], 3);
///         assert_eq!(buf[3], 4);
///         assert_eq!(buf[4], 5);
///         let mut x = x.write().unwrap();
///         (*x) += 1;
///         Ok(())
///     })?;
///
///     // start the event handler
///     eh.start()?;
///
///     // add the tcp listener
///     eh.add_tcp_listener(&listener)?;
///     // get the write handle which is needed to write on the connection
///     let wh = eh.add_tcp_stream(&stream)?;
///     // send the message
///     wh.write(&[1, 2, 3, 4, 5])?;
///     // wait long enough to make sure the client got the message
///     std::thread::sleep(std::time::Duration::from_millis(1000));
///     let x = x_clone.write().unwrap();
///     // ensure that the client callback executed
///     assert_eq!((*x), 1);
///     Ok(())
/// }
/// ```
pub struct EventHandler<F, G, H, K> {
	config: EventHandlerConfig,
	guarded_data: Vec<Arc<RwLock<GuardedData>>>,
	callbacks: Arc<RwLock<Callbacks<F, G, H, K>>>,
	global_lock: Arc<RwLock<bool>>,
	on_panic: Option<OnPanic>,
	tls_server_config: Option<ServerConfig>,
}

impl<F, G, H, K> EventHandler<F, G, H, K>
where
	F: Fn(&[u8], usize, WriteHandle) -> Result<(), Error> + Send + 'static + Clone + Sync + Unpin,
	G: Fn(u128, WriteHandle) -> Result<(), Error> + Send + 'static + Clone + Sync,
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
			let callbacks = nioruntime_util::lockr!(self.callbacks);

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
				log_multi!(
					ERROR,
					MAIN_LOG,
					"setsockopt resulted in error: {}",
					errno().to_string()
				);
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
		let (fd, connection_id) = self.add(stream.as_raw_fd(), ActionType::AddStream)?;
		#[cfg(target_os = "windows")]
		let (fd, connection_id) = self.add(stream.as_raw_socket(), ActionType::AddStream)?;

		let callback_state = Arc::new(RwLock::new(State::Init));
		Ok(WriteHandle {
			fd,
			connection_id,
			guarded_data: self.guarded_data[1].clone(), // TODO: not always 1.
			global_lock: self.global_lock.clone(),
			callback_state,
			tls_conn: None,
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
		#[cfg(unix)]
		self.add(listener.as_raw_fd(), ActionType::AddListener)?;
		#[cfg(target_os = "windows")]
		self.add(
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
	/// use nioruntime_evh::{EventHandler, EventHandlerConfig};
	/// use nioruntime_util::Error;
	///
	/// fn main() -> Result<(), Error> {
	///     let mut eh = EventHandler::new(EventHandlerConfig::default());
	///     // set the on_read callback to simply echo back what is written to it
	///     eh.set_on_read(|buf, len, wh| {
	///         let _ = wh.write(&buf[0..len]);
	///         Ok(())
	///     })?;
	///     eh.set_on_accept(|_,_| Ok(()))?;
	///     eh.set_on_client_read(|_,_,_| Ok(()))?;
	///     eh.set_on_close(|_| Ok(()))?;
	///     Ok(())
	/// }
	/// ```
	pub fn set_on_read(&mut self, on_read: F) -> Result<(), Error> {
		let mut callbacks = nioruntime_util::lockw!(self.callbacks);

		callbacks.on_read = Some(Box::pin(on_read));

		Ok(())
	}

	pub fn set_on_panic(&mut self, on_panic: OnPanic) -> Result<(), Error> {
		self.on_panic = Some(on_panic);
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
	/// use nioruntime_log::*;
	///
	/// // set log level to info
	/// info!();
	///
	/// fn main() -> Result<(), Error> {
	///     let mut eh = EventHandler::new(nioruntime_evh::EventHandlerConfig::default());
	///     // print out a message when a new connection is accepted
	///     eh.set_on_accept(|connection_id, _| {
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
		let mut callbacks = nioruntime_util::lockw!(self.callbacks);

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
	/// use nioruntime_log::*;
	///
	/// // set log level to info
	/// info!();
	///
	/// fn main() -> Result<(), Error> {
	///     let mut eh = EventHandler::new(nioruntime_evh::EventHandlerConfig::default());
	///     // print out a message when a connection is closed
	///     eh.set_on_close(|connection_id| {
	///         info!("closed connection with id = {}", connection_id);
	///         Ok(())
	///     })?;
	///     eh.set_on_read(|_,_,_|  Ok(()))?;
	///     eh.set_on_client_read(|_,_,_| Ok(()))?;
	///     eh.set_on_accept(|_,_| Ok(()))?;
	///     Ok(())
	/// }
	/// ```
	pub fn set_on_close(&mut self, on_close: H) -> Result<(), Error> {
		let mut callbacks = nioruntime_util::lockw!(self.callbacks);

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
	/// use nioruntime_log::*;
	///
	/// // set log level to info
	/// info!();
	///
	/// fn main() -> Result<(), Error> {
	///     let mut eh = EventHandler::new(nioruntime_evh::EventHandlerConfig::default());
	///     // echo back the message
	///     eh.set_on_client_read(|buf, len, wh| {
	///         let _ = wh.write(&buf[0..len]);
	///         Ok(())
	///     })?;
	///     eh.set_on_close(|_| Ok(()))?;
	///     eh.set_on_read(|_,_,_|  Ok(()))?;
	///     eh.set_on_accept(|_,_| Ok(()))?;
	///     Ok(())
	/// }
	/// ```
	pub fn set_on_client_read(&mut self, on_client_read: K) -> Result<(), Error> {
		let mut callbacks = nioruntime_util::lockw!(self.callbacks);

		callbacks.on_client_read = Some(Box::pin(on_client_read));

		Ok(())
	}

	/// Create a new instance of the [`EventHandler`]. Note that all callbacks must be registered
	/// and [`EventHandler::start`] must be called before handleing events. See the example in the
	/// [`EventHandler`] section of the documentation for a full details.
	pub fn new(config: EventHandlerConfig) -> Self {
		let callbacks = Callbacks {
			on_read: None,
			on_accept: None,
			on_close: None,
			on_client_read: None,
		};
		let callbacks = Arc::new(RwLock::new(callbacks));

		let mut guarded_data = vec![];
		for _ in 0..config.thread_count + 1 {
			guarded_data.push(Arc::new(RwLock::new(GuardedData {
				nconns: vec![],
				write_queue: vec![],
				aconns: vec![],
				cconns: vec![],
				wakeup_tx: 0,
				wakeup_rx: 0,
				wakeup_scheduled: false,
				stop: false,
				callback_state: Arc::new(RwLock::new(State::Init)),
			})));
		}

		let global_lock = Arc::new(RwLock::new(true));

		let tls_server_config = if config.tls_config.is_some() {
			let tls_config = config.tls_config.as_ref().unwrap();
			match ServerConfig::builder()
				.with_safe_defaults()
				.with_no_client_auth()
				.with_single_cert(
					load_certs(&tls_config.certificates_file),
					load_private_key(&tls_config.private_key_file),
				) {
				Ok(tls_server_config) => Some(tls_server_config),
				Err(e) => {
					panic!("Error loading tls configurations: {}", e.to_string());
				}
			}
		} else {
			None
		};

		EventHandler {
			config,
			guarded_data,
			callbacks,
			global_lock,
			on_panic: None,
			tls_server_config,
		}
	}

	/// Start the event handler.
	pub fn start(&mut self) -> Result<(), Error> {
		for i in 0..self.guarded_data.len() {
			let (rx, tx) = Self::build_pipe()?;
			let mut guarded_data = nioruntime_util::lockw!(self.guarded_data[i]);
			guarded_data.wakeup_tx = tx;
			guarded_data.wakeup_rx = rx;
		}
		self.do_start()?;

		Ok(())
	}

	/// Stop the event handler and free any internal resources associated with it. Note: this
	/// does not close any registered sockets. That is the responsibility of the user.
	pub fn stop(&self) -> Result<(), Error> {
		for i in 0..self.guarded_data.len() {
			let mut guarded_data = nioruntime_util::lockw!(self.guarded_data[i]);
			guarded_data.stop = true;
			guarded_data.wakeup()?;
		}

		Ok(())
	}

	fn build_pipe() -> Result<(ConnectionHandle, ConnectionHandle), Error> {
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
		Ok((retfds[0], retfds[1]))
	}

	#[cfg(target_os = "linux")]
	fn do_start(&mut self) -> Result<(), Error> {
		let mut selectors = vec![];
		for _ in 0..self.config.thread_count + 1 {
			selectors.push(epoll_create1(EpollCreateFlags::empty()).unwrap());
		}
		self.start_generic(selectors)?;
		Ok(())
	}

	#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
	fn do_start(&mut self) -> Result<(), Error> {
		let mut selectors = vec![];
		for _ in 0..self.config.thread_count + 1 {
			selectors.push(unsafe { kqueue() });
		}
		self.start_generic(selectors)?;
		Ok(())
	}

	#[cfg(target_os = "windows")]
	fn do_start(&mut self) -> Result<(), Error> {
		let mut selectors = vec![];
		for _ in 0..self.config.thread_count + 1 {
			selectors.push(unsafe { kqueue() });
		}
		self.start_generic(selectors)?;
		Ok(())
	}

	fn ensure_handlers(&self) -> Result<(), Error> {
		let callbacks = nioruntime_util::lockr!(self.callbacks);

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

	fn add(
		&mut self,
		handle: ConnectionHandle,
		atype: ActionType,
	) -> Result<(ConnectionHandle, u128), Error> {
		let mut rng = rand::thread_rng();
		let connection_id = rng.gen();

		let (tx, rx) = sync_channel(5);

		let conn = ConnectionInfo {
			handle,
			connection_id,
			ctype: if atype == ActionType::AddListener {
				ConnectionType::Listener
			} else {
				ConnectionType::Outbound
			},
			sender: Some(tx.clone()),
			tls_conn: None,
		};

		{
			match atype {
				ActionType::AddListener => {
					let mut guarded_data = nioruntime_util::lockw!(self.guarded_data[0]);
					guarded_data.nconns.push(conn);
					guarded_data.wakeup()?;
				}
				ActionType::AddStream => {
					let mut guarded_data = nioruntime_util::lockw!(self.guarded_data[1]);
					guarded_data.nconns.push(conn);
					guarded_data.wakeup()?;
				}
			}
		}

		rx.recv().map_err(|e| {
			let error: Error =
				ErrorKind::InternalError(format!("recv error: {}", e.to_string())).into();
			error
		})?;

		Ok((handle, connection_id))
	}

	fn start_generic(&mut self, selectors: Vec<SelectorHandle>) -> Result<(), Error> {
		self.ensure_handlers()?;

		let on_panic = self.on_panic.clone();
		let global_lock = &self.global_lock;
		let global_lock_clone = global_lock.clone();
		let selectors_clone = selectors.clone();
		let guarded_data = self.guarded_data[0].clone();
		let callbacks = nioruntime_util::lockr!(self.callbacks);
		let mut guarded_data_vec = vec![];
		for i in 1..self.guarded_data.len() {
			guarded_data_vec.push(self.guarded_data[i].clone());
		}

		let on_accept = callbacks.on_accept.as_ref().unwrap().clone();
		let on_close = callbacks.on_close.as_ref().unwrap().clone();
		let tls_server_config = self.tls_server_config.clone();
		spawn(move || {
			match Self::listener(
				selectors_clone[0],
				guarded_data,
				&mut guarded_data_vec,
				on_accept.clone(),
				on_close.clone(),
				global_lock_clone.clone(),
				tls_server_config,
			) {
				Ok(_) => {}
				Err(e) => {
					log_multi!(
						ERROR,
						MAIN_LOG,
						"listener generated error: {}",
						e.to_string()
					);
				}
			}
		});

		// start r/w threads
		for i in 0..self.config.thread_count {
			let selectors = selectors.clone();
			let listener_guarded_data = self.guarded_data[0].clone();
			let guarded_data = self.guarded_data[i + 1].clone();
			let on_read = callbacks.on_read.as_ref().unwrap().clone();
			let on_client_read = callbacks.on_client_read.as_ref().unwrap().clone();
			let global_lock = global_lock.clone();
			let connection_id_map = Arc::new(RwLock::new(HashMap::new()));
			let connection_info_map = Arc::new(RwLock::new(HashMap::new()));
			let hash_set = Arc::new(RwLock::new(HashSet::new()));
			let write_buffers = Arc::new(RwLock::new(HashMap::new()));
			let input_events = Arc::new(RwLock::new(Vec::new()));
			let output_events = Arc::new(RwLock::new(Vec::new()));
			let on_panic = on_panic.clone();
			let counter = Arc::new(RwLock::new(0));
			let res = Arc::new(RwLock::new(0));

			let wakeup_fd;

			// add wakeup fd
			{
				let guarded_data = nioruntime_util::lockw!(guarded_data);
				let mut input_events = nioruntime_util::lockw!(input_events);

				input_events.push(GenericEvent {
					fd: guarded_data.wakeup_rx,
					etype: GenericEventType::AddReadLT,
				});

				wakeup_fd = guarded_data.wakeup_rx;
			}

			spawn(move || loop {
				let selectors = selectors.clone();
				let listener_guarded_data = listener_guarded_data.clone();
				let guarded_data = guarded_data.clone();
				let guarded_data_clone = guarded_data.clone();
				let on_read = on_read.clone();
				let on_client_read = on_client_read.clone();
				let on_panic = on_panic.clone();
				let global_lock = global_lock.clone();
				let connection_id_map = connection_id_map.clone();
				let connection_info_map = connection_info_map.clone();
				let write_buffers = write_buffers.clone();
				let hash_set = hash_set.clone();
				let input_events = input_events.clone();
				let output_events = output_events.clone();
				let counter = counter.clone();
				let res = res.clone();
				let jh = spawn(move || {
					match Self::rwthread(
						selectors[i + 1],
						listener_guarded_data,
						guarded_data_clone,
						on_read,
						on_client_read,
						global_lock,
						connection_info_map,
						connection_id_map,
						hash_set,
						write_buffers,
						wakeup_fd,
						input_events,
						output_events,
						counter,
						res,
					) {
						Ok(_) => {}
						Err(e) => {
							log_multi!(
								ERROR,
								MAIN_LOG,
								"rwthread generated error: {}",
								e.to_string()
							);
						}
					}
				});

				{
					let guarded_data = guarded_data.write().unwrap();
					if guarded_data.stop {
						break;
					}
				}

				match jh.join() {
					Ok(_) => {}
					Err(_e) => {
						log_multi!(ERROR, MAIN_LOG, "thread panic!");
					}
				}

				match on_panic {
					Some(on_panic) => match (on_panic)() {
						Ok(_) => {}
						Err(e) => {
							println!("on_panic generated error: {}", e.to_string());
						}
					},
					None => {}
				}
			});
		}
		Ok(())
	}

	fn update_listener_input_events(
		guarded_data: &Arc<RwLock<GuardedData>>,
		input_events: &mut Vec<GenericEvent>,
		global_lock: Arc<RwLock<bool>>,
		on_close: Pin<Box<H>>,
		cid_map: &mut HashMap<ConnectionHandle, u128>,
	) -> Result<bool, Error> {
		let stop;
		let nconns;
		let cconns;
		{
			let mut guarded_data = nioruntime_util::lockw!(guarded_data);
			stop = guarded_data.stop;
			nconns = guarded_data.nconns.clone();
			cconns = guarded_data.cconns.clone();
			guarded_data.nconns.clear();
			guarded_data.cconns.clear();
		}

		for conn in nconns {
			let ge = GenericEvent {
				fd: conn.handle,
				etype: GenericEventType::AddReadLT,
			};
			input_events.push(ge);

			if conn.sender.is_some() {
				let _ = conn.sender.unwrap().send(());
			}
		}

		let _lock = nioruntime_util::lockw!(global_lock);
		for conn in cconns {
			let connection_id = conn.connection_id;
			let fd = conn.handle;
			(on_close)(connection_id)?;

			let lookup = cid_map.remove(&fd);
			if lookup.is_none() {
				continue;
			}
			let lookup = lookup.unwrap();
			if lookup != connection_id {
				continue;
			}

			{
				#[cfg(unix)]
				let res = unsafe { close(fd) };
				#[cfg(target_os = "windows")]
				let res = unsafe { ws2_32::closesocket(fd) };
				if res != 0 {
					let e = errno();
					info!("error closing socket: {}", e.to_string());
					return Err(ErrorKind::InternalError("Already closed".to_string()).into());
				}
			}
		}

		Ok(stop)
	}

	fn process_listener_events(
		count: usize,
		events: &Vec<GenericEvent>,
		next_index: &mut usize,
		guarded_data: &mut Vec<Arc<RwLock<GuardedData>>>,
		on_accept: Pin<Box<G>>,
		global_lock: Arc<RwLock<bool>>,
		wakeup_fd: ConnectionHandle,
		cid_map: &mut HashMap<ConnectionHandle, u128>,
		tls_server_config: Option<ServerConfig>,
	) -> Result<(), Error> {
		for i in 0..count {
			let event = &events[i];
			if event.fd == wakeup_fd {
				// don't process here. Process in the main loop
				continue;
			}

			let res = {
				let _lock = nioruntime_util::lockw!(global_lock);
				#[cfg(unix)]
				let res = unsafe {
					accept(
						event.fd,
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
						event.fd,
						&mut winapi::ws2def::SOCKADDR {
							..std::mem::zeroed()
						},
						&mut (std::mem::size_of::<winapi::ws2def::SOCKADDR>() as u32)
							.try_into()
							.unwrap_or(0),
					)
				};

				res
			};

			if res > 0 {
				// set non-blocking
				#[cfg(unix)]
				{
					let fcntl_res = unsafe { fcntl(res, libc::F_SETFL, libc::O_NONBLOCK) };
					if fcntl_res < 0 {
						let e = errno().to_string();
						return Err(ErrorKind::InternalError(format!("fcntl error: {}", e)).into());
					}
				}
				#[cfg(target_os = "windows")]
				{
					let fionbio = 0x8004667eu32;
					let ioctl_res = unsafe { ws2_32::ioctlsocket(res, fionbio as c_int, &mut 1) };

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
				let index = *next_index % guarded_data.len();
				*next_index += 1;
				if *next_index == usize::MAX {
					*next_index = 0;
				}
				let guarded_data_next = &guarded_data[index];
				let mut rng = rand::thread_rng();
				let connection_id = rng.gen();

				let tls_conn = match tls_server_config.clone() {
					Some(tls_server_config) => {
						//let tls_config =
						//	Arc::new(ServerConfig::clone(config.tls_config.as_ref().unwrap()));
						let tls_config = Arc::new(tls_server_config);

						match ServerConnection::new(tls_config) {
							Ok(tls_conn) => Some(Arc::new(RwLock::new(tls_conn))),
							Err(e) => {
								error!("Error building tls_connection: {}", e.to_string());
								None
							}
						}
					}
					None => None,
				};

				let wh = WriteHandle::new(
					res,
					guarded_data[index].clone(),
					connection_id,
					global_lock.clone(),
					tls_conn.clone(),
				);
				(on_accept)(connection_id, wh)?;
				cid_map.insert(res, connection_id);
				{
					let mut guarded_data_next = nioruntime_util::lockw!(guarded_data_next);

					guarded_data_next.nconns.push(ConnectionInfo {
						handle: res,
						connection_id,
						ctype: ConnectionType::Inbound,
						sender: None,
						tls_conn,
					});
					guarded_data_next.wakeup()?;
				}
			}
		}

		Ok(())
	}

	fn listener(
		selector: SelectorHandle,
		guarded_data: Arc<RwLock<GuardedData>>,
		guarded_data_vec: &mut Vec<Arc<RwLock<GuardedData>>>,
		on_accept: Pin<Box<G>>,
		on_close: Pin<Box<H>>,
		global_lock: Arc<RwLock<bool>>,
		tls_server_config: Option<ServerConfig>,
	) -> Result<(), Error> {
		let mut cid_map = HashMap::new();
		let mut hash_set = HashSet::new();
		let mut input_events = vec![];
		let mut output_events = vec![];
		let mut next_index = 0;
		let wakeup_fd;

		// add wakeup fd
		{
			let guarded_data = nioruntime_util::lockw!(guarded_data);
			input_events.push(GenericEvent {
				fd: guarded_data.wakeup_rx,
				etype: GenericEventType::AddReadLT,
			});
			wakeup_fd = guarded_data.wakeup_rx;
		}

		let mut wakeup = false;

		loop {
			// get new handles
			let stop = Self::update_listener_input_events(
				&guarded_data,
				&mut input_events,
				global_lock.clone(),
				on_close.clone(),
				&mut cid_map,
			)?;

			if stop {
				#[cfg(unix)]
				let _ = unsafe { close(selector) };
				#[cfg(target_os = "windows")]
				let _ = unsafe { ws2_32::closesocket(selector) };
				break;
			}

			output_events.clear();
			let count = Self::get_events(
				selector,
				input_events.clone(),
				&mut output_events,
				&mut hash_set,
				wakeup,
			)?;

			{
				let mut guarded_data = nioruntime_util::lockw!(guarded_data);
				if guarded_data.wakeup_scheduled {
					wakeup = true;
					guarded_data.wakeup_scheduled = false;
					let cbuf: *mut c_void = &mut [0u8; 1] as *mut _ as *mut c_void;
					let res = unsafe { read(wakeup_fd, cbuf, 1) };
					if res <= 0 {
						log_multi!(ERROR, MAIN_LOG, "read error on wakeupfd");
					}
				} else {
					wakeup = false;
				}
			}

			input_events.clear();

			Self::process_listener_events(
				count,
				&output_events,
				&mut next_index,
				guarded_data_vec,
				on_accept.clone(),
				global_lock.clone(),
				wakeup_fd,
				&mut cid_map,
				tls_server_config.clone(),
			)?;
		}
		Ok(())
	}

	fn update_rw_input_events(
		guarded_data: Arc<RwLock<GuardedData>>,
		input_events: &mut Vec<GenericEvent>,
		connection_info_map: &mut HashMap<ConnectionHandle, ConnectionInfo>,
		connection_id_map: &mut HashMap<u128, ConnectionInfo>,
		hash_set: &mut HashSet<ConnectionHandle>,
		global_lock: Arc<RwLock<bool>>,
		on_read: Pin<Box<F>>,
	) -> Result<bool, Error> {
		let nconns;
		let stop;
		let aconns;
		{
			let mut guarded_data = nioruntime_util::lockw!(guarded_data);
			stop = guarded_data.stop;
			nconns = guarded_data.nconns.clone();
			aconns = guarded_data.aconns.clone();
			guarded_data.aconns.clear();
			guarded_data.nconns.clear();
		}

		for conn in aconns {
			let wh = WriteHandle::new(
				conn.handle,
				guarded_data.clone(),
				conn.connection_id,
				global_lock.clone(),
				conn.tls_conn,
			);
			(on_read)(&[0u8; 0], 0, wh)?;
		}

		for conn in nconns {
			hash_set.remove(&conn.handle);
			connection_info_map.insert(conn.handle, conn.clone());
			connection_id_map.insert(conn.connection_id, conn.clone());
			let ge = GenericEvent {
				fd: conn.handle,
				etype: GenericEventType::AddReadET,
			};
			input_events.push(ge);

			match conn.tls_conn {
				Some(_tls_conn) => {
					let ge = GenericEvent {
						fd: conn.handle,
						etype: GenericEventType::AddWriteET,
					};
					input_events.push(ge);
				}
				None => {}
			}

			if conn.sender.is_some() {
				let _ = conn.sender.unwrap().send(());
			}
		}

		Ok(stop)
	}

	fn do_write(
		fd: ConnectionHandle,
		write_buffer: &mut WriteBuffer,
		global_lock: Arc<RwLock<bool>>,
	) -> Result<(bool, bool, bool), Error> {
		if write_buffer.len == 0 {
			return Ok((false, write_buffer.close, true));
		}

		let len = write_data(
			fd,
			&mut write_buffer.buffer[(write_buffer.offset as usize)
				..(write_buffer.offset as usize + write_buffer.len as usize)],
			&global_lock,
		)?;

		if len >= 0 {
			if len == (write_buffer.len as i32).try_into().unwrap_or(0) {
				// we're done
				write_buffer.offset += len as u16;
				write_buffer.len -= len as u16;
				return Ok((true, write_buffer.close, false));
			} else {
				// update values and write again
				write_buffer.offset += len as u16;
				write_buffer.len -= len as u16;
				return Ok((false, false, false));
			}
		} else {
			let error = errno();
			if errno().0 == EAGAIN {
				errno::set_errno(Errno(0));
				// break because we're edge triggered.
				// a new event occurs.
				return Ok((false, false, true));
			} else {
				// this is an actual write error.
				// close the connection.
				info!("write error: {}", error.to_string());
				return Ok((true, true, true));
			}
		}
	}

	fn process_write_event(
		selector: SelectorHandle,
		event: &GenericEvent,
		connection_id: u128,
		write_buffers: &mut HashMap<u128, LinkedList<WriteBuffer>>,
		global_lock: Arc<RwLock<bool>>,
		connection_id_map: &mut HashMap<u128, ConnectionInfo>,
		connection_info_map: &mut HashMap<ConnectionHandle, ConnectionInfo>,
		listener_guarded_data: Arc<RwLock<GuardedData>>,
		filter_set: &mut HashSet<ConnectionHandle>,
	) -> Result<(), Error> {
		let mut disconnect = false;
		let list = write_buffers.get_mut(&connection_id);
		let mut break_received = false;
		match list {
			Some(list) => loop {
				if list.is_empty() || break_received {
					break;
				}

				loop {
					let front = list.front_mut();
					match front {
						Some(mut front) => {
							let (pop, disc, br) =
								Self::do_write(event.fd, &mut front, global_lock.clone())?;
							if disc {
								break_received = true;
								disconnect = true;
								break;
							}
							if pop {
								// if all was written, pop
								list.pop_front();
							}
							if br {
								break_received = true;
								break;
							}
						}
						None => {
							break;
						}
					}
				}
			},
			None => {}
		}

		if disconnect {
			let fd = event.fd;
			if connection_id_map.remove(&connection_id).is_some() {
				connection_info_map.remove(&fd);
				write_buffers.remove(&connection_id);
				Self::remove_handle(selector, fd, filter_set)?;

				let mut listener_guarded_data = nioruntime_util::lockw!(listener_guarded_data);
				listener_guarded_data.cconns.push(ConnectionInfo {
					handle: fd,
					connection_id,
					ctype: ConnectionType::Inbound,
					sender: None,
					tls_conn: None,
				});
				listener_guarded_data.wakeup()?;
			}
		}

		Ok(())
	}

	fn process_read_result(
		selector: SelectorHandle,
		fd: ConnectionHandle,
		listener_guarded_data: Arc<RwLock<GuardedData>>,
		guarded_data: Arc<RwLock<GuardedData>>,
		buf: &[u8],
		len: isize,
		connection_id: u128,
		on_read: Pin<Box<F>>,
		on_client_read: Pin<Box<K>>,
		connection_id_map: &mut HashMap<u128, ConnectionInfo>,
		connection_info_map: &mut HashMap<ConnectionHandle, ConnectionInfo>,
		write_buffers: &mut HashMap<u128, LinkedList<WriteBuffer>>,
		global_lock: Arc<RwLock<bool>>,
		filter_set: &mut HashSet<ConnectionHandle>,
	) -> Result<bool, Error> {
		let connection_info = connection_id_map.get(&connection_id);
		if connection_info.is_some() {
			let connection_info = connection_info.unwrap();
			if len > 0 {
				let wh = WriteHandle::new(
					fd,
					guarded_data,
					connection_id,
					global_lock,
					connection_info.tls_conn.clone(),
				);
				match connection_info.ctype {
					ConnectionType::Inbound => (on_read)(buf, len.try_into().unwrap_or(0), wh)?,
					ConnectionType::Outbound => {
						(on_client_read)(buf, len.try_into().unwrap_or(0), wh)?
					}
					_ => {} // not expected
				}
				Ok(true)
			} else {
				let mut do_close = true;
				let e = errno();
				if len < 0 {
					if e.0 == EAGAIN {
						errno::set_errno(Errno(0));
						do_close = false;
					}
				}

				if do_close {
					if connection_id_map.remove(&connection_id).is_some() {
						connection_info_map.remove(&fd);
						write_buffers.remove(&connection_id);
						Self::remove_handle(selector, fd, filter_set)?;

						let mut listener_guarded_data =
							nioruntime_util::lockw!(listener_guarded_data);
						listener_guarded_data.cconns.push(ConnectionInfo {
							handle: fd,
							connection_id,
							ctype: ConnectionType::Inbound,
							sender: None,
							tls_conn: None,
						});
						listener_guarded_data.wakeup()?;
					}
				}
				Ok(false)
			}
		} else {
			if len > 0 {
				error!(
					"error reading {} bytes on unknown fd = {}, cid={}",
					len, fd, connection_id
				);
			}
			Ok(false)
		}
	}

	fn process_writes(
		guarded_data: Arc<RwLock<GuardedData>>,
		write_buffers: &mut HashMap<u128, LinkedList<WriteBuffer>>,
		input_events: &mut Vec<GenericEvent>,
		connection_id_map: &mut HashMap<u128, ConnectionInfo>,
	) -> Result<(), Error> {
		let write_queue = {
			let mut guarded_data = nioruntime_util::lockw!(guarded_data);
			let ret = guarded_data.write_queue.clone();
			guarded_data.write_queue.clear();
			ret
		};

		let mut hash_set = HashSet::new();

		for write_buffer in write_queue {
			let list = write_buffers.get_mut(&write_buffer.connection_id);
			let connection_info = connection_id_map.get(&write_buffer.connection_id);

			match connection_info {
				Some(connection_info) => {
					match list {
						Some(list) => {
							list.push_back(write_buffer);
						}
						None => {
							let mut list = LinkedList::new();
							list.push_back(write_buffer);
							write_buffers.insert(connection_info.connection_id, list);
						}
					}
					let ge = GenericEvent {
						fd: connection_info.handle,
						etype: GenericEventType::AddWriteET,
					};
					if hash_set.get(&connection_info.handle).is_none() {
						hash_set.insert(connection_info.handle);
						input_events.push(ge);
					}
				}
				None => {
					// the connection already closed
					log_multi!(
						INFO,
						MAIN_LOG,
						"connection not found write_buffer: '{}', map='{:?}'",
						write_buffer.connection_id,
						connection_id_map
					);
				}
			}
		}

		Ok(())
	}

	fn rwthread(
		selector: SelectorHandle,
		listener_guarded_data: Arc<RwLock<GuardedData>>,
		guarded_data: Arc<RwLock<GuardedData>>,
		on_read: Pin<Box<F>>,
		on_client_read: Pin<Box<K>>,
		global_lock: Arc<RwLock<bool>>,
		connection_info_map: Arc<RwLock<HashMap<ConnectionHandle, ConnectionInfo>>>,
		connection_id_map: Arc<RwLock<HashMap<u128, ConnectionInfo>>>,
		hash_set: Arc<RwLock<HashSet<ConnectionHandle>>>,
		write_buffers: Arc<RwLock<HashMap<u128, LinkedList<WriteBuffer>>>>,
		wakeup_fd: ConnectionHandle,
		input_events: Arc<RwLock<Vec<GenericEvent>>>,
		output_events: Arc<RwLock<Vec<GenericEvent>>>,
		counter: Arc<RwLock<usize>>,
		res: Arc<RwLock<usize>>,
	) -> Result<(), Error> {
		let mut connection_info_map = nioruntime_util::lockwp!(connection_info_map);
		let mut connection_id_map = nioruntime_util::lockwp!(connection_id_map);
		let mut hash_set = nioruntime_util::lockwp!(hash_set);
		let mut write_buffers = nioruntime_util::lockwp!(write_buffers);
		let mut input_events = nioruntime_util::lockwp!(input_events);
		let mut output_events = nioruntime_util::lockwp!(output_events);
		let mut counter = nioruntime_util::lockwp!(counter);
		let mut res = nioruntime_util::lockwp!(res);

		// handle panic in progress events here
		*counter += 1;
		Self::process_events(
			selector,
			&mut counter,
			&mut res,
			&output_events,
			wakeup_fd,
			listener_guarded_data.clone(),
			&mut connection_info_map,
			&mut connection_id_map,
			&mut write_buffers,
			global_lock.clone(),
			on_read.clone(),
			on_client_read.clone(),
			guarded_data.clone(),
			&mut hash_set,
		)?;

		if *res != 0 {
			input_events.clear();
		}
		output_events.clear();

		let mut wakeup = false;

		loop {
			// see if there's any new write buffers to process
			Self::process_writes(
				guarded_data.clone(),
				&mut write_buffers,
				&mut input_events,
				&mut connection_id_map,
			)?;

			// get new handles
			let stop = Self::update_rw_input_events(
				guarded_data.clone(),
				&mut input_events,
				&mut connection_info_map,
				&mut connection_id_map,
				&mut hash_set,
				global_lock.clone(),
				on_read.clone(),
			)?;

			if stop {
				#[cfg(unix)]
				let _ = unsafe { close(selector) };
				#[cfg(target_os = "windows")]
				let _ = unsafe { ws2_32::closesocket(selector) };
				break;
			}

			*res = Self::get_events(
				selector,
				input_events.clone(),
				&mut output_events,
				&mut hash_set,
				wakeup,
			)?;

			{
				let mut guarded_data = nioruntime_util::lockw!(guarded_data);
				if guarded_data.wakeup_scheduled {
					wakeup = true;
					guarded_data.wakeup_scheduled = false;
					let cbuf: *mut c_void = &mut [0u8; 1] as *mut _ as *mut c_void;
					let res = unsafe { read(wakeup_fd, cbuf, 1) };
					if res <= 0 {
						log_multi!(ERROR, MAIN_LOG, "read error on wakeupfd");
					}
				} else {
					wakeup = false;
				}
			}

			*counter = 0;
			Self::process_events(
				selector,
				&mut counter,
				&mut res,
				&output_events,
				wakeup_fd,
				listener_guarded_data.clone(),
				&mut connection_info_map,
				&mut connection_id_map,
				&mut write_buffers,
				global_lock.clone(),
				on_read.clone(),
				on_client_read.clone(),
				guarded_data.clone(),
				&mut hash_set,
			)?;

			output_events.clear();
			input_events.clear();
		}
		Ok(())
	}

	fn process_events(
		selector: SelectorHandle,
		counter: &mut RwLockWriteGuard<usize>,
		res: &mut RwLockWriteGuard<usize>,
		output_events: &Vec<GenericEvent>,
		wakeup_fd: ConnectionHandle,
		listener_guarded_data: Arc<RwLock<GuardedData>>,
		connection_info_map: &mut HashMap<ConnectionHandle, ConnectionInfo>,
		connection_id_map: &mut HashMap<u128, ConnectionInfo>,
		write_buffers: &mut HashMap<u128, LinkedList<WriteBuffer>>,
		global_lock: Arc<RwLock<bool>>,
		on_read: Pin<Box<F>>,
		on_client_read: Pin<Box<K>>,
		guarded_data: Arc<RwLock<GuardedData>>,
		filter_set: &mut HashSet<ConnectionHandle>,
	) -> Result<(), Error> {
		for i in **counter..**res {
			match output_events[i].etype {
				GenericEventType::AddReadET | GenericEventType::AddReadLT => {
					if output_events[i].fd == wakeup_fd {
						// ignore wakeupfd here process in main loop.
					} else {
						let conn_info = connection_info_map.get(&output_events[i].fd);
						match conn_info {
							Some(conn_info) => {
								let handle = conn_info.handle;
								let connection_id = conn_info.connection_id;
								match conn_info.tls_conn.clone() {
									Some(mut tls_conn) => loop {
										let mut buf = vec![];
										let (raw_len, tls_len) = Self::do_tls_read(
											handle,
											connection_id,
											guarded_data.clone(),
											&mut buf,
											global_lock.clone(),
											&mut tls_conn,
										)?;
										if raw_len <= 0 || tls_len > 0 {
											let len = if tls_len > 0 {
												tls_len.try_into().unwrap_or(0)
											} else {
												raw_len
											};

											if !Self::process_read_result(
												selector,
												handle,
												listener_guarded_data.clone(),
												guarded_data.clone(),
												&buf,
												len,
												connection_id,
												on_read.clone(),
												on_client_read.clone(),
												connection_id_map,
												connection_info_map,
												write_buffers,
												global_lock.clone(),
												filter_set,
											)? {
												break;
											}
										}
									},
									None => loop {
										let mut buf = [0u8; BUFFER_SIZE];
										let len =
											Self::do_read(handle, &mut buf, global_lock.clone())?;
										if !Self::process_read_result(
											selector,
											handle,
											listener_guarded_data.clone(),
											guarded_data.clone(),
											&buf,
											len,
											connection_id,
											on_read.clone(),
											on_client_read.clone(),
											connection_id_map,
											connection_info_map,
											write_buffers,
											global_lock.clone(),
											filter_set,
										)? {
											break;
										}
									},
								}
							}
							None => {
								// looks to be spurious. connection already disconnected
								log_multi!(
									DEBUG,
									MAIN_LOG,
									"connection not found (add read): {}",
									output_events[i].fd
								);
							}
						}
					}
				}
				GenericEventType::AddWriteET => {
					let conn_info = connection_info_map.get(&output_events[i].fd);

					match conn_info {
						Some(conn_info) => {
							Self::process_write_event(
								selector,
								&output_events[i],
								conn_info.connection_id,
								write_buffers,
								global_lock.clone(),
								connection_id_map,
								connection_info_map,
								listener_guarded_data.clone(),
								filter_set,
							)?;
						}
						None => {
							// looks to be spurious. connection already disconnected
							log_multi!(
								DEBUG,
								MAIN_LOG,
								"connection not found (add write): {}",
								output_events[i].fd
							);
						}
					}
				} //_ => {}
			}
			**counter += 1;
		}
		Ok(())
	}

	fn do_tls_read(
		handle: ConnectionHandle,
		connection_id: u128,
		guarded_data: Arc<RwLock<GuardedData>>,
		buf: &mut Vec<u8>,
		global_lock: Arc<RwLock<bool>>,
		tls_conn: &mut Arc<RwLock<ServerConnection>>,
	) -> Result<(isize, usize), Error> {
		let pt_len;
		buf.resize(BUFFER_SIZE, 0u8);
		let len = Self::do_read(handle, buf, global_lock.clone())?;

		let mut wbuf = vec![];
		{
			let mut tls_conn = nioruntime_util::lockw!(tls_conn);

			tls_conn.read_tls(&mut &buf[0..len.try_into().unwrap_or(0)])?;

			match tls_conn.process_new_packets() {
				Ok(io_state) => {
					pt_len = io_state.plaintext_bytes_to_read();
					buf.resize(pt_len, 0u8);
					tls_conn.reader().read_exact(buf)?;
				}
				Err(e) => {
					warn!(
						"error generated processing packets for handle={}. Error={}",
						handle,
						e.to_string()
					);
					return Ok((-1, 0)); // invalid text received. Close conn.
				}
			}
			tls_conn.write_tls(&mut wbuf)?;
		}

		let wh = WriteHandle::new(
			handle,
			guarded_data.clone(),
			connection_id,
			global_lock.clone(),
			None,
		);
		wh.write(&wbuf)?;

		Ok((len, pt_len))
	}

	fn do_read(
		handle: ConnectionHandle,
		buf: &mut [u8],
		global_lock: Arc<RwLock<bool>>,
	) -> Result<isize, Error> {
		let _lock = nioruntime_util::lockr!(global_lock);
		#[cfg(unix)]
		let len = {
			let cbuf: *mut c_void = buf as *mut _ as *mut c_void;
			unsafe { read(handle, cbuf, BUFFER_SIZE) }
		};
		#[cfg(target_os = "windows")]
		let len = {
			let cbuf: *mut i8 = &mut buf as *mut _ as *mut i8;
			unsafe { ws2_32::recv(handle, cbuf, BUFFER_SIZE.try_into().unwrap_or(0), 0) }
		};
		Ok(len)
	}

	#[cfg(target_os = "windows")]
	fn remove_handle(
		selector: SelectorHandle,
		connection_handle: ConnectionHandle,
		filter_set: &mut HashSet<ConnectionHandle>,
	) -> Result<(), Error> {
		Ok(())
	}

	#[cfg(target_os = "linux")]
	fn remove_handle(
		selector: SelectorHandle,
		connection_handle: ConnectionHandle,
		filter_set: &mut HashSet<ConnectionHandle>,
	) -> Result<(), Error> {
		filter_set.remove(&connection_handle);
		let interest = EpollFlags::empty();

		let mut event = EpollEvent::new(interest, connection_handle.try_into().unwrap_or(0));
		let res = epoll_ctl(
			selector,
			EpollOp::EpollCtlDel,
			connection_handle,
			&mut event,
		);
		match res {
			Ok(_) => {}
			Err(e) => info!("Error epoll_ctl4: {}, fd={}, delete", e, connection_handle),
		}

		Ok(())
	}

	#[cfg(any(target_os = "macos", dragonfly, freebsd, netbsd, openbsd))]
	fn remove_handle(
		selector: SelectorHandle,
		connection_handle: ConnectionHandle,
		_filter_set: &mut HashSet<ConnectionHandle>,
	) -> Result<(), Error> {
		let mut kevs = vec![];
		kevs.push(kevent::new(
			connection_handle as uintptr_t,
			EventFilter::EVFILT_READ,
			EventFlag::EV_DELETE,
			FilterFlag::empty(),
		));

		let mut ret_kevs = vec![];
		let ret_count = unsafe {
			kevent(
				selector,
				kevs.as_ptr(),
				kevs.len() as i32,
				ret_kevs.as_mut_ptr(),
				0,
				&duration_to_timespec(std::time::Duration::from_millis(0)),
			)
		};

		if ret_count < 0 {
			info!(
				"Error in kevent (remove handle): kevs={:?}, error={}",
				kevs,
				errno()
			);
		}
		Ok(())
	}

	#[cfg(target_os = "windows")]
	fn get_events(
		selector: SelectorHandle,
		input_events: Vec<GenericEvent>,
		output_events: &mut Vec<GenericEvent>,
		filter_set: &mut HashSet<ConnectionHandle>,
		wakeup: bool,
	) -> Result<usize, Error> {
		for evt in input_events {
			if evt.etype == GenericEventType::AddReadET
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
						EPOLL_CTL_DEL.try_into().unwrap_or(0),
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
		let results = unsafe { epoll_wait(win_selector, events.as_mut_ptr(), MAX_EVENTS, 3000) };
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
		selector: SelectorHandle,
		input_events: Vec<GenericEvent>,
		output_events: &mut Vec<GenericEvent>,
		filter_set: &mut HashSet<ConnectionHandle>,
		wakeup: bool,
	) -> Result<usize, Error> {
		let epollfd = selector;
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
			} else {
				return Err(
					ErrorKind::InternalError(format!("unexpected etype: {:?}", evt.etype)).into(),
				);
			}
		}

		let empty_event = EpollEvent::new(EpollFlags::empty(), 0);
		let mut events = [empty_event; MAX_EVENTS as usize];
		let results = epoll_wait(
			epollfd,
			&mut events,
			match wakeup {
				true => 0,
				false => 3000,
			},
		);

		let mut ret_count_adjusted = output_events.len();

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
		selector: SelectorHandle,
		input_events: Vec<GenericEvent>,
		output_events: &mut Vec<GenericEvent>,
		_filter_set: &HashSet<ConnectionHandle>,
		wakeup: bool,
	) -> Result<usize, Error> {
		let queue = selector;
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
				&duration_to_timespec(std::time::Duration::from_millis(match wakeup {
					true => 1,
					false => 3000,
				})),
			)
		};

		if ret_count < 0 {
			info!("Error in kevent: kevs={:?}, error={}", kevs, errno());
		}

		let mut ret_count_adjusted = output_events.len();
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
						GenericEventType::AddReadLT,
					));
				}
			}
		}

		Ok(ret_count_adjusted)
	}
}

#[derive(Clone, Debug)]
enum ConnectionType {
	Outbound,
	Inbound,
	Listener,
}

#[derive(Clone, Debug)]
struct ConnectionInfo {
	handle: ConnectionHandle,
	connection_id: u128,
	ctype: ConnectionType,
	sender: Option<SyncSender<()>>,
	tls_conn: Option<Arc<RwLock<ServerConnection>>>,
}

struct GuardedData {
	nconns: Vec<ConnectionInfo>,
	cconns: Vec<ConnectionInfo>,
	aconns: Vec<ConnectionInfo>,
	write_queue: Vec<WriteBuffer>,
	wakeup_tx: ConnectionHandle,
	wakeup_rx: ConnectionHandle,
	wakeup_scheduled: bool,
	stop: bool,
	callback_state: Arc<RwLock<State>>,
}

impl GuardedData {
	fn wakeup(&mut self) -> Result<(), Error> {
		if !self.wakeup_scheduled {
			let buf: *mut c_void = &mut [0u8; 1] as *mut _ as *mut c_void;
			let res = unsafe { write(self.wakeup_tx, buf, 1) };
			if res <= 0 {
				log_multi!(ERROR, MAIN_LOG, "Error writing to wakup tx {}", res);
			}
			self.wakeup_scheduled = true;
		}
		Ok(())
	}
}

#[derive(Debug, Clone)]
pub(crate) struct WriteBuffer {
	len: u16,
	offset: u16,
	buffer: [u8; BUFFER_SIZE],
	close: bool,
	connection_id: u128,
}

struct Callbacks<F, G, H, K> {
	on_read: Option<Pin<Box<F>>>,
	on_accept: Option<Pin<Box<G>>>,
	on_close: Option<Pin<Box<H>>>,
	on_client_read: Option<Pin<Box<K>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamWriteBuffer {
	fd: ConnectionHandle,
	write_buffer: WriteBuffer,
}

#[derive(Debug, Clone, PartialEq)]
enum ActionType {
	AddStream,
	AddListener,
}

#[derive(Debug, PartialEq, Clone, Eq, Hash)]
enum GenericEventType {
	AddReadET,
	AddReadLT,
	//	DelRead,
	AddWriteET,
	//	DelWrite,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
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
				//GenericEventType::DelRead => EventFilter::EVFILT_READ,
				GenericEventType::AddWriteET => EventFilter::EVFILT_WRITE,
				//GenericEventType::DelWrite => EventFilter::EVFILT_WRITE,
			},
			match &self.etype {
				GenericEventType::AddReadET => EventFlag::EV_ADD | EventFlag::EV_CLEAR,
				GenericEventType::AddReadLT => EventFlag::EV_ADD,
				//GenericEventType::DelRead => EventFlag::EV_DELETE,
				GenericEventType::AddWriteET => EventFlag::EV_ADD | EventFlag::EV_CLEAR,
				//GenericEventType::DelWrite => EventFlag::EV_DELETE,
			},
			FilterFlag::empty(),
		)
	}
}

fn write_data(
	handle: ConnectionHandle,
	buf: &[u8],
	global_lock: &Arc<RwLock<bool>>,
) -> Result<isize, Error> {
	let _lock = nioruntime_util::lockr!(global_lock);
	#[cfg(unix)]
	let len = {
		let cbuf: *const c_void = buf as *const _ as *const c_void;
		unsafe { write(handle, cbuf, buf.len().into()) }
	};
	#[cfg(target_os = "windows")]
	let len = {
		let cbuf: *const i8 = &buf as *const _ as *const i8;
		unsafe { ws2_32::send(handle, cbuf, (buf.len()).into(), 0) }
	};

	Ok(len)
}

#[cfg(unix)]
type SelectorHandle = i32;
#[cfg(target_os = "windows")]
type SelectorHandle = *mut c_void;

#[test]
fn test_echo() -> Result<(), Error> {
	use std::net::TcpListener;
	use std::net::TcpStream;
	use std::sync::Mutex;

	let x = Arc::new(Mutex::new(0));
	let x_clone = x.clone();

	let listener = TcpListener::bind("127.0.0.1:9981")?;
	let stream = TcpStream::connect("127.0.0.1:9981")?;
	let mut eh = EventHandler::new(EventHandlerConfig::default());

	// echo
	eh.set_on_read(|buf, len, wh| {
		info!("server received: {} bytes", len);
		let _ = wh.write(&buf[0..len]);
		Ok(())
	})?;

	eh.set_on_accept(|_, _| Ok(()))?;
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
	// TODO: would be nice to eliminate this sleep
	wh.write(&[1, 2, 3, 4, 5])?;
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
	use std::sync::Mutex;

	let listener = TcpListener::bind("127.0.0.1:9982")?;
	let mut stream = TcpStream::connect("127.0.0.1:9982")?;
	let mut eh = EventHandler::new(EventHandlerConfig::default());

	let read_buf = Arc::new(Mutex::new(vec![]));
	eh.set_on_read(move |buf, len, wh| {
		let mut read_buf = read_buf.lock().unwrap();
		for i in 0..len {
			read_buf.push(buf[i]);
		}

		if read_buf.len() < 5 {
			return Ok(());
		}
		info!("read = {:?}", read_buf);

		let len = read_buf.len();

		for i in 0..len {
			if read_buf[i] == 7 {
				wh.close()?;
				return Ok(());
			}
		}
		for i in 0..len {
			if read_buf[i] == 8 {
				wh.write(&[0])?;
				wh.close()?;
				read_buf.clear();
				return Ok(());
			}
		}
		wh.write(&[1])?;
		read_buf.clear();
		Ok(())
	})?;

	eh.set_on_accept(|_, _| Ok(()))?;
	eh.set_on_close(|_| Ok(()))?;
	eh.set_on_client_read(move |_buf, _len, _wh| Ok(()))?;

	eh.start()?;
	eh.add_tcp_listener(&listener)?;
	stream.write(&[1, 2, 3, 4, 5])?;
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
	let mut eh = EventHandler::new(EventHandlerConfig::default());

	// echo
	eh.set_on_read(|buf, len, wh| {
		match len {
			// just close the connection with no response
			7 => {
				let _ = wh.close();
			}
			// close if len == 5, otherwise keep open
			_ => {
				let _ = wh.write(&buf[0..len])?;
				if len == 5 {
					wh.close()?;
				}
			}
		}
		Ok(())
	})?;

	eh.set_on_accept(|_, _| Ok(()))?;
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
	use std::sync::Mutex;

	let listener = TcpListener::bind("127.0.0.1:9984")?;
	let mut stream = TcpStream::connect("127.0.0.1:9984")?;
	let mut eh = EventHandler::new(EventHandlerConfig::default());
	let x = Arc::new(Mutex::new(0));
	let xclone = x.clone();

	// echo
	eh.set_on_read(move |_, _, _| {
		let mut x = xclone.lock().unwrap();
		*x += 1;
		Ok(())
	})?;

	eh.set_on_accept(|_, _| Ok(()))?;
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
	use std::sync::Mutex;

	let listener = TcpListener::bind("127.0.0.1:9933")?;
	let stream = TcpStream::connect("127.0.0.1:9933")?;
	let mut eh = EventHandler::new(EventHandlerConfig::default());

	eh.set_on_read(move |buf, len, wh| {
		match len {
			// just close the connection with no response
			7 => {
				let _ = wh.close();
			}
			// close if len == 5, otherwise keep open
			_ => {
				let _ = wh.write(&buf[0..len])?;
				if len == 5 {
					wh.close()?;
				}
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

	eh.set_on_accept(|_, _| Ok(()))?;
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
	wh.write(&msgbuf[0..msgbuf.len()])?;
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
