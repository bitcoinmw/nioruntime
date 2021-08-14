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

use dirs;
use log::*;
use nioruntime_evh::{EventHandler, WriteHandle};
use nioruntime_util::threadpool::StaticThreadPool;
use nioruntime_util::{Error, ErrorKind};
use rand::Rng;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::metadata;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

debug!();

const VERSION: &'static str = env!("CARGO_PKG_VERSION");
const MAX_CHUNK_SIZE: u64 = 10 * 1024 * 1024;

#[derive(Debug)]
enum HttpVersion {
	V10,
	V11,
}

#[derive(Clone)]
pub struct HttpConfig {
	pub host: String,
	pub port: u16,
	pub root_dir: String,
	pub thread_pool_size: usize,
	pub server_name: String,
}

impl Default for HttpConfig {
	fn default() -> HttpConfig {
		HttpConfig {
			host: "0.0.0.0".to_string(),
			port: 8080,
			root_dir: "~/.niohttpd".to_string(),
			thread_pool_size: 6,
			server_name: "NIORuntime Httpd".to_string(),
		}
	}
}

impl HttpConfig {
	pub fn new(
		host: String,
		port: u16,
		root_dir: String,
		thread_pool_size: usize,
		server_name: String,
	) -> Self {
		HttpConfig {
			root_dir,
			port,
			host,
			thread_pool_size,
			server_name,
		}
	}
}

struct ConnData {
	buffer: Vec<u8>,
}

impl ConnData {
	fn new() -> Self {
		ConnData { buffer: vec![] }
	}
}

struct HttpContext {
	stop: bool,
	map: Arc<RwLock<HashMap<u128, Arc<RwLock<ConnData>>>>>,
}

impl HttpContext {
	fn new() -> Self {
		HttpContext {
			stop: false,
			map: Arc::new(RwLock::new(HashMap::new())),
		}
	}
}

pub struct HttpServer {
	config: HttpConfig,
	listener: Option<TcpListener>,
	context: Option<Arc<RwLock<HttpContext>>>,
	thread_pool: Option<Arc<RwLock<StaticThreadPool>>>,
}

impl HttpServer {
	pub fn new(config: HttpConfig) -> Self {
		let mut cloned_config = config.clone();
		let home_dir = match dirs::home_dir() {
			Some(p) => p,
			None => PathBuf::new(),
		}
		.as_path()
		.display()
		.to_string();
		cloned_config.root_dir = config.root_dir.replace("~", &home_dir);

		let exists = match std::fs::metadata(cloned_config.root_dir.clone()) {
			Ok(md) => md.is_dir(),
			Err(_) => false,
		};

		if !exists {
			match Self::build_webroot(cloned_config.root_dir.clone()) {
				Ok(_) => {}
				Err(e) => {
					error!("building webroot generated error: {}", e.to_string());
				}
			}
		}

		HttpServer {
			config: cloned_config,
			listener: None,
			context: None,
			thread_pool: None,
		}
	}

	fn create_file_from_bytes(
		resource: String,
		root_dir: String,
		bytes: &[u8],
	) -> Result<(), Error> {
		//let bytes = include_bytes!(format!("resources/{}", resource));
		let path = format!("{}/www/{}", root_dir, resource);
		let mut file = File::create(&path)?;
		file.write_all(bytes)?;
		Ok(())
	}

	pub fn start(&mut self) -> Result<(), Error> {
		let addr = format!("{}:{}", self.config.host, self.config.port,);
		let listener = TcpListener::bind(addr.clone())?;
		info!("Server Started on: {}", addr);
		info!("root_dir: {}", self.config.root_dir);

		let http_config = self.config.clone();
		let http_config_clone = http_config.clone();
		let http_config_clone2 = http_config.clone();

		let thread_pool = StaticThreadPool::new()?;
		thread_pool.start(self.config.thread_pool_size)?;
		let thread_pool = Arc::new(RwLock::new(thread_pool));
		let thread_pool_clone = thread_pool.clone();
		let thread_pool_clone2 = thread_pool.clone();
		let thread_pool_clone3 = thread_pool.clone();

		let http_context = HttpContext::new();

		let http_context = Arc::new(RwLock::new(http_context));
		let http_context_clone = http_context.clone();
		let http_context_clone2 = http_context.clone();
		let http_context_clone3 = http_context.clone();
		let http_context_clone4 = http_context.clone();
		let mut eh = EventHandler::new();

		eh.set_on_read(move |buf, len, wh| {
			Self::process_read(
				thread_pool_clone.clone(),
				http_context.clone(),
				http_config.clone(),
				buf,
				len,
				wh,
			)
		})?;
		eh.set_on_accept(move |id| {
			Self::process_accept(
				thread_pool_clone2.clone(),
				http_context_clone.clone(),
				http_config_clone.clone(),
				id,
			)
		})?;
		eh.set_on_client_read(|_, _, _| Ok(()))?;
		eh.set_on_close(move |id| {
			Self::process_close(
				thread_pool_clone3.clone(),
				http_context_clone2.clone(),
				http_config_clone2.clone(),
				id,
			)
		})?;

		eh.start()?;
		eh.add_tcp_listener(&listener)?;
		self.listener = Some(listener);
		self.context = Some(http_context_clone3);
		self.thread_pool = Some(thread_pool);

		std::thread::spawn(move || loop {
			std::thread::sleep(std::time::Duration::from_millis(100));
			{
				let http_context = match http_context_clone4.read() {
					Ok(http_context) => http_context,
					Err(e) => {
						error!(
							"unexpected error obtaining lock on http_context: {}",
							e.to_string()
						);
						break;
					}
				};

				if http_context.stop {
					info!("Stopping HttpServer");
					let stop_res = eh.stop();
					match stop_res {
						Ok(_) => {}
						Err(e) => {
							error!("unexpected error stopping eventhandler: {}", e.to_string())
						}
					}
					break;
				}
			}
		});

		Ok(())
	}

	pub fn stop(&mut self) -> Result<(), Error> {
		match &self.context {
			Some(context) => {
				let mut http_context = context.write().map_err(|_e| {
					let error: Error = ErrorKind::InternalError(
						"unexpected error obtaining http_context lock".to_string(),
					)
					.into();
					error
				})?;
				http_context.stop = true;
			}
			None => {
				warn!("Tried to stop an HttpServer that never started.");
			}
		}

		match self.listener.as_ref() {
			Some(listener) => drop(listener),
			None => {}
		}
		self.listener = None;
		Ok(())
	}

	fn process_accept(
		_thread_pool: Arc<RwLock<StaticThreadPool>>,
		http_context: Arc<RwLock<HttpContext>>,
		_http_config: HttpConfig,
		id: u128,
	) -> Result<(), Error> {
		let http_context = http_context.write().map_err(|e| {
			let error: Error =
				ErrorKind::PoisonError(format!("unexpected error: {}", e.to_string())).into();
			error
		})?;

		let mut map = http_context.map.write().map_err(|e| {
			let error: Error =
				ErrorKind::PoisonError(format!("unexpected error: {}", e.to_string())).into();
			error
		})?;

		map.insert(id, Arc::new(RwLock::new(ConnData::new())));
		Ok(())
	}

	fn process_read(
		thread_pool: Arc<RwLock<StaticThreadPool>>,
		http_context: Arc<RwLock<HttpContext>>,
		http_config: HttpConfig,
		buf: &[u8],
		len: usize,
		wh: WriteHandle,
	) -> Result<(), Error> {
		let conn_context = {
			let http_context = http_context.write().map_err(|e| {
				let error: Error =
					ErrorKind::PoisonError(format!("unexpected error: {}", e.to_string())).into();
				error
			})?;

			let mut map = http_context.map.write().map_err(|e| {
				let error: Error =
					ErrorKind::PoisonError(format!("unexpected error: {}", e.to_string())).into();
				error
			})?;

			let conn_context = map.get_mut(&wh.get_connection_id());

			match conn_context {
				Some(context) => context.clone(),
				None => {
					error!(
						"unexpected error getting ConnData for {}",
						wh.get_connection_id()
					);
					return Err(ErrorKind::InternalError(
						"unexpected error getting ConnData".to_string(),
					)
					.into());
				}
			}
		};

		let thread_pool = thread_pool.read().map_err(|e| {
			let error: Error =
				ErrorKind::PoisonError(format!("unexpected error: {}", e.to_string())).into();
			error
		})?;

		{
			let mut conn_context = conn_context.write().map_err(|e| {
				let error: Error =
					ErrorKind::PoisonError(format!("unexpected error: {}", e.to_string())).into();
				error
			})?;

			for i in 0..len {
				conn_context.buffer.push(buf[i]);
			}
		}

		thread_pool.execute(async move {
			match Self::process_request(&http_config, conn_context, wh) {
				Ok(_) => {}
				Err(e) => {
					error!("unexpected error processing request: {}", e.to_string());
				}
			}
		})?;

		Ok(())
	}

	fn process_request(
		config: &HttpConfig,
		conn_context: Arc<RwLock<ConnData>>,
		wh: WriteHandle,
	) -> Result<(), Error> {
		let mut conn_context = conn_context
			.write()
			.map_err(|e| {
				let error: Error =
					ErrorKind::PoisonError(format!("unexpected error: {}", e.to_string())).into();
				error
			})
			.unwrap();
		let buffer = &mut conn_context.buffer;
		// iterate through and try to find a double line break. If we find it,
		// send to next function for processing and delete the data that we send.
		loop {
			let len = buffer.len();
			if len <= 3 {
				break;
			}
			let mut response_sent = false;
			for i in 3..len {
				if (buffer[i - 3] == '\r' as u8
				&& buffer[i - 2] == '\n' as u8
				&& buffer[i - 1] == '\r' as u8
				&& buffer[i] == '\n' as u8)
				// we are tolerant of the incorrect format too
				|| (buffer[i - 1] == '\n' as u8 && buffer[i] == '\n' as u8)
				{
					// end of a request found.
					response_sent = true;
					if len < 6
						|| (buffer[0..4] != ['G' as u8, 'E' as u8, 'T' as u8, ' ' as u8]
							&& (buffer[0..5]
								!= ['P' as u8, 'O' as u8, 'S' as u8, 'T' as u8, ' ' as u8]))
					{
						Self::send_bad_request_error(&wh)?;
					} else {
						let mut space_count = 0;
						let mut uri = vec![];
						let mut http_ver_string = vec![];

						for j in 0..len {
							if buffer[j] == ' ' as u8
								|| buffer[j] == '\r' as u8 || buffer[j] == '\n' as u8
							{
								space_count += 1;
							}
							if space_count == 1 {
								if buffer[j] != ' ' as u8 {
									uri.push(buffer[j]);
								}
							} else if space_count == 2 {
								if buffer[j] != ' ' as u8 {
									http_ver_string.push(buffer[j]);
								}
							} else if space_count > 2 {
								break;
							}
						}

						// get headers
						let mut nl_count = 0;
						let mut headers_vec = vec![];
						for j in 0..len {
							if buffer[j] == '\n' as u8 {
								nl_count += 1;
								if j + 2 < len
									&& buffer[j + 1] == '\r' as u8 && buffer[j + 2] == '\n' as u8
								{
									break;
								}
								headers_vec.push(vec![]);
							}
							if buffer[j] != '\n' as u8 && nl_count > 0 {
								headers_vec[nl_count - 1].push(buffer[j]);
							}
						}

						let mut sep_headers_vec = vec![];
						let headers_vec_len = headers_vec.len();
						let mut keep_alive = false;
						for j in 0..headers_vec_len {
							let header_j_len = headers_vec[j].len();
							sep_headers_vec.push((vec![], vec![]));
							let mut found_sep = false;
							let mut found_sep_plus_1 = false;
							for k in 0..header_j_len {
								if headers_vec[j][k] == ':' as u8 {
									found_sep = true;
								} else if !found_sep {
									sep_headers_vec[j].0.push(headers_vec[j][k]);
								} else {
									if found_sep_plus_1 {
										if headers_vec[j][k] != '\r' as u8
											&& headers_vec[j][k] != '\n' as u8
										{
											sep_headers_vec[j].1.push(headers_vec[j][k]);
										}
									}
									found_sep_plus_1 = true;
								}
							}
							let header = std::str::from_utf8(&sep_headers_vec[j].0[..])?;
							let value = std::str::from_utf8(&sep_headers_vec[j].1[..])?;
							if header == "Connection" && value == "keep-alive" {
								keep_alive = true;
							}
						}

						let http_ver_string = std::str::from_utf8(&http_ver_string[..])?;
						let http_version =
							if http_ver_string == "HTTP/1.1" || http_ver_string == "HTTP/2.0" {
								HttpVersion::V11 // we use 1.1 responses for 2.0 for now
							} else {
								keep_alive = false;
								HttpVersion::V10
							};
						info!("uri = {}", std::str::from_utf8(&uri[..])?);
						Self::send_response(
							&config,
							&wh,
							http_version,
							std::str::from_utf8(&uri[..])?,
							sep_headers_vec,
							keep_alive,
						)?;
					}

					for _ in 0..i + 1 {
						buffer.remove(0);
					}

					break;
				}
			}
			if !response_sent {
				break;
			}
		}
		Ok(())
	}

	fn send_bad_request_error(wh: &WriteHandle) -> Result<(), Error> {
		let num: u32 = rand::thread_rng().gen();
		let msg = format!(
			"HTTP/1.1 400 Bad Request\r\n\r\nInvalid Request {}.\r\n\r\n",
			num
		);
		let response = msg.as_bytes();
		wh.write(response, 0, response.len(), true)?;
		Ok(())
	}

	fn send_response(
		config: &HttpConfig,
		wh: &WriteHandle,
		version: HttpVersion,
		uri: &str,
		headers: Vec<(Vec<u8>, Vec<u8>)>,
		keep_alive: bool,
	) -> Result<(), Error> {
		let mut uri_path = vec![];
		let mut query_string = vec![];
		let mut start_query = false;
		let uri = uri.as_bytes();
		for i in 0..uri.len() {
			if !start_query && uri[i] != '?' as u8 {
				uri_path.push(uri[i]);
			} else if !start_query && uri[i] == '?' as u8 {
				start_query = true;
			} else {
				query_string.push(uri[i]);
			}
		}
		let uri = std::str::from_utf8(&uri_path[..])?;
		let query = std::str::from_utf8(&query_string[..])?;

		let mut path = format!("{}/www{}", config.root_dir, uri);
		let mut flen = match metadata(path.clone()) {
			Ok(md) => {
				if md.is_dir() {
					path = format!("{}/www{}/index.html", config.root_dir, uri);
					match metadata(path.clone()) {
						Ok(md) => md.len(),
						Err(_) => {
							path = format!("{}/www/404.html", config.root_dir);
							match metadata(path.clone()) {
								Ok(md) => md.len(),
								Err(_) => 0,
							}
						}
					}
				} else {
					md.len()
				}
			}
			Err(_) => {
				// try to return the 404 page.
				path = format!("{}/www/404.html", config.root_dir);
				match metadata(path.clone()) {
					Ok(md) => md.len(),
					Err(_) => 0,
				}
			}
		};

		let file = File::open(path.clone());

		match file {
			Ok(mut file) => {
				let buflen = if flen > MAX_CHUNK_SIZE {
					MAX_CHUNK_SIZE
				} else {
					flen
				};

				let mut buf = vec![0; buflen as usize];
				let mut first_loop = true;
				loop {
					let res = file.read(&mut buf);
					match res {
						Ok(amt) => {
							if first_loop {
								Self::write_headers(wh, config, true, keep_alive)?;
							}
							if keep_alive {
								let msg_len_bytes = format!("{:X}\r\n", amt);
								let msg_len_bytes = msg_len_bytes.as_bytes();
								wh.write(msg_len_bytes, 0, msg_len_bytes.len(), false)?;
								wh.write(&buf, 0, amt, false)?;
								if flen <= amt.try_into().unwrap_or(0) {
									wh.write("\r\n0\r\n\r\n".as_bytes(), 0, 7, !keep_alive)?;
								} else {
									wh.write("\r\n".as_bytes(), 0, 2, false)?;
								}
							} else {
								if flen <= amt.try_into().unwrap_or(0) {
									wh.write(&buf, 0, amt, !keep_alive)?;
								} else {
									wh.write(&buf, 0, amt, false)?;
								}
							}
							flen -= amt.try_into().unwrap_or(0);
						}
						Err(e) => {
							// directory
							Self::write_headers(wh, config, false, keep_alive)?;
							break;
						}
					}

					if flen <= 0 {
						break;
					}
					first_loop = false;
				}
			}
			Err(_) => {
				// file not found
				Self::write_headers(wh, config, false, keep_alive)?;
			}
		}

		Ok(())
	}

	fn write_headers(
		wh: &WriteHandle,
		config: &HttpConfig,
		found: bool,
		keep_alive: bool,
	) -> Result<(), Error> {
		let response_404 = "<html><body>404 Page not found!</body></html>".to_string();
		let now = chrono::Utc::now();

		let date = format!("Date: {}\r\n", now.format("%a, %d %h %Y %T GMT"));
		let server = format!("Server: {} {}\r\n", config.server_name, VERSION);
		let transfer_encoding = if found && keep_alive {
			"Transfer-Encoding: chunked\r\n".to_string()
		} else if !found {
			format!("Content-Length: {}\r\n", response_404.len())
		} else {
			"".to_string()
		};

		let not_found_message = if found {
			"".to_string()
		} else {
			let response = response_404;
			format!("{}\r\n", response)
		};

		let response = format!(
			"{}{}{}{}\r\n{}",
			if found {
				"HTTP/1.1 200 OK\r\n"
			} else {
				"HTTP/1.1 404 Not Found\r\n"
			},
			date,
			server,
			transfer_encoding,
			not_found_message,
		);

		let response = response.as_bytes();
		wh.write(response, 0, response.len(), false)?;
		if !found && !keep_alive {
			wh.close()?;
		}

		Ok(())
	}

	fn process_close(
		_thread_pool: Arc<RwLock<StaticThreadPool>>,
		http_context: Arc<RwLock<HttpContext>>,
		_http_config: HttpConfig,
		id: u128,
	) -> Result<(), Error> {
		let http_context = http_context.write().map_err(|e| {
			let error: Error =
				ErrorKind::PoisonError(format!("unexpected error: {}", e.to_string())).into();
			error
		})?;

		let mut map = http_context.map.write().map_err(|e| {
			let error: Error =
				ErrorKind::PoisonError(format!("unexpected error: {}", e.to_string())).into();
			error
		})?;

		map.remove(&id);
		Ok(())
	}

	fn build_webroot(root_dir: String) -> Result<(), Error> {
		fsutils::mkdir(&root_dir);
		fsutils::mkdir(&format!("{}/www", root_dir));
		fsutils::mkdir(&format!("{}/logs", root_dir));

		let bytes = include_bytes!("resources/DERP.jpeg");
		match Self::create_file_from_bytes("DERP.jpeg".to_string(), root_dir.clone(), bytes) {
			Ok(_) => {}
			Err(e) => {
				error!("Creating file resulted in error: {}", e.to_string());
			}
		}

		let bytes = include_bytes!("resources/index.html");
		match Self::create_file_from_bytes("index.html".to_string(), root_dir.clone(), bytes) {
			Ok(_) => {}
			Err(e) => {
				error!("Creating file resulted in error: {}", e.to_string());
			}
		}

		let bytes = include_bytes!("resources/404.html");
		match Self::create_file_from_bytes("404.html".to_string(), root_dir.clone(), bytes) {
			Ok(_) => {}
			Err(e) => {
				error!("Creating file resulted in error: {}", e.to_string());
			}
		}

		let bytes = include_bytes!("resources/favicon.ico");
		match Self::create_file_from_bytes("favicon.ico".to_string(), root_dir.clone(), bytes) {
			Ok(_) => {}
			Err(e) => {
				error!("Creating file resulted in error: {}", e.to_string());
			}
		}

		let bytes = include_bytes!("resources/favicon-16x16.png");
		match Self::create_file_from_bytes("favicon-16x16.png".to_string(), root_dir.clone(), bytes)
		{
			Ok(_) => {}
			Err(e) => {
				error!("Creating file resulted in error: {}", e.to_string());
			}
		}

		let bytes = include_bytes!("resources/favicon-32x32.png");
		match Self::create_file_from_bytes("favicon-32x32.png".to_string(), root_dir.clone(), bytes)
		{
			Ok(_) => {}
			Err(e) => {
				error!("Creating file resulted in error: {}", e.to_string());
			}
		}

		let bytes = include_bytes!("resources/about.txt");
		match Self::create_file_from_bytes("about.txt".to_string(), root_dir.clone(), bytes) {
			Ok(_) => {}
			Err(e) => {
				error!("Creating file resulted in error: {}", e.to_string());
			}
		}

		let bytes = include_bytes!("resources/android-chrome-192x192.png");
		match Self::create_file_from_bytes(
			"android-chrome-192x192.png".to_string(),
			root_dir.clone(),
			bytes,
		) {
			Ok(_) => {}
			Err(e) => {
				error!("Creating file resulted in error: {}", e.to_string());
			}
		}

		let bytes = include_bytes!("resources/android-chrome-512x512.png");
		match Self::create_file_from_bytes(
			"android-chrome-512x512.png".to_string(),
			root_dir.clone(),
			bytes,
		) {
			Ok(_) => {}
			Err(e) => {
				error!("Creating file resulted in error: {}", e.to_string());
			}
		}

		let bytes = include_bytes!("resources/apple-touch-icon.png");
		match Self::create_file_from_bytes(
			"apple-touch-icon.png".to_string(),
			root_dir.clone(),
			bytes,
		) {
			Ok(_) => {}
			Err(e) => {
				error!("Creating file resulted in error: {}", e.to_string());
			}
		}

		let bytes = include_bytes!("resources/lettherebebitcom.png");
		match Self::create_file_from_bytes(
			"lettherebebitcom.png".to_string(),
			root_dir.clone(),
			bytes,
		) {
			Ok(_) => {}
			Err(e) => {
				error!("Creating file resulted in error: {}", e.to_string());
			}
		}

		Ok(())
	}
}
