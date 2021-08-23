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

debug!();

#[cfg(unix)]
use libc::close;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

// windows specific deps
#[cfg(target_os = "windows")]
use std::os::windows::io::AsRawSocket;

use byte_tools::copy;
use byteorder::{LittleEndian, ReadBytesExt};
use clap::load_yaml;
use clap::App;
use errno::errno;
use nioruntime_evh::eventhandler::EventHandler;
use nioruntime_http::HttpConfig;
use nioruntime_http::HttpServer;
use nioruntime_log::*;
use nioruntime_util::Error;
use rand::Rng;
use std::collections::HashMap;
use std::convert::TryInto;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Mutex;

const MAX_BUF: usize = 100_000;

struct Buffer {
	data: [u8; MAX_BUF],
	len: usize,
}

impl Buffer {
	fn new() -> Self {
		let data = [0u8; MAX_BUF];
		let len = 0;
		Buffer { data, len }
	}
}

// include build information
pub mod built_info {
	include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

fn main() {
	let res = real_main();
	match res {
		Ok(_) => {}
		Err(e) => error!("real_main generated Error: {}", e.to_string()),
	}
}

fn client_thread(
	count: usize,
	id: usize,
	tlat_sum: Arc<Mutex<f64>>,
	tlat_max: Arc<Mutex<u128>>,
	min: u32,
	max: u32,
) -> Result<(), Error> {
	let mut lat_sum = 0.0;
	let mut lat_max = 0;
	let (mut stream, fd) = {
		let _lock = tlat_sum.lock();
		let stream = TcpStream::connect("127.0.0.1:9999")?;
		#[cfg(unix)]
		let fd = stream.as_raw_fd();
		#[cfg(target_os = "windows")]
		let fd = stream.as_raw_socket();
		(stream, fd)
	};
	let buf = &mut [0u8; MAX_BUF];
	let buf2 = &mut [0u8; MAX_BUF];
	let start_itt = std::time::SystemTime::now();
	for i in 0..count {
		if i != 0 && i % 10000 == 0 {
			let elapsed = start_itt.elapsed().unwrap().as_millis();
			let qps = (i as f64 / elapsed as f64) * 1000 as f64;
			info!("Request {} on thread {}, qps={}", i, id, qps);
		}
		let start_query = std::time::SystemTime::now();
		let num: u32 = rand::thread_rng().gen_range(min..max);
		let num_buf = num.to_le_bytes();
		let offt: u8 = rand::thread_rng().gen_range(0..64);
		copy(&num_buf[0..4], &mut buf[0..4]);
		buf[4] = offt;
		let offt = offt as u32;
		for i in 0..num {
			buf[i as usize + 5] = ((i + offt) % 128) as u8;
		}
		let res = stream.write(&buf[0..(num as usize + 5)]);

		match res {
			Ok(_x) => {}
			Err(e) => {
				info!("Write Error: {}", e.to_string());
				std::thread::sleep(std::time::Duration::from_millis(1));
			}
		}

		let mut len_sum = 0;
		loop {
			let res = stream.read(&mut buf2[len_sum..]);
			match res {
				Ok(_) => {}
				Err(ref e) => {
					info!("Read Error: {}, fd = {}", e.to_string(), fd);
					assert!(false);
				}
			}
			let len = res.unwrap();
			len_sum += len;
			if len_sum == num as usize + 5 {
				break;
			}
		}

		if num == 99990 {
			// we expect a close here. Try one more read
			let len = stream.read(&mut buf2[0..])?;
			// len should be 0
			assert_eq!(len, 0);
			// not that only a single request is currently supported in this mode.
			// TODO: support reconnect
			info!("Successful disconnect");
		}

		let elapsed = start_query.elapsed().unwrap().as_nanos();
		lat_sum += elapsed as f64;
		if elapsed > lat_max {
			lat_max = elapsed;
		}

		assert_eq!(len_sum, num as usize + 5);
		assert_eq!(Cursor::new(&buf2[0..4]).read_u32::<LittleEndian>()?, num);
		assert_eq!(buf2[4], offt as u8);
		for i in 0..num {
			if buf2[i as usize + 5] != ((i + offt) % 128) as u8 {
				info!("assertion at {} fails", i);
			}
			assert_eq!(buf2[i as usize + 5], ((i + offt) % 128) as u8);
		}
		// clear buf2
		for i in 0..len_sum {
			buf2[i] = 0;
		}
	}

	{
		let _lock = tlat_sum.lock();
		#[cfg(unix)]
		let close_res = unsafe { close(fd.try_into().unwrap_or(0)) };
		#[cfg(target_os = "windows")]
		let close_res = unsafe { ws2_32::closesocket(fd.try_into().unwrap_or(0)) };
		if close_res != 0 {
			let e = errno();
			info!("error close {} (fd={})", e.to_string(), fd);
		}
		drop(stream);
	}

	{
		let mut tlat_sum = tlat_sum.lock().unwrap();
		(*tlat_sum) += lat_sum;
	}
	{
		let mut tlat_max = tlat_max.lock().unwrap();
		if lat_max > *tlat_max {
			(*tlat_max) = lat_max;
		}
	}

	Ok(())
}

fn real_main() -> Result<(), Error> {
	log_config!(nioruntime_log::LogConfig::default())?;

	let yml = load_yaml!("nio.yml");
	let args = App::from_yaml(yml)
		.version(built_info::PKG_VERSION)
		.get_matches();
	let client = args.is_present("client");
	let threads = args.is_present("threads");
	let count = args.is_present("count");
	let itt = args.is_present("itt");
	let max = args.is_present("max");
	let min = args.is_present("min");
	let http = args.is_present("http");

	let threads = match threads {
		true => args.value_of("threads").unwrap().parse().unwrap(),
		false => 1,
	};

	let count = match count {
		true => args.value_of("count").unwrap().parse().unwrap(),
		false => 1,
	};

	let itt = match itt {
		true => args.value_of("itt").unwrap().parse().unwrap(),
		false => 1,
	};

	let max = match max {
		true => args.value_of("max").unwrap().parse().unwrap(),
		false => 100,
	};

	let min = match min {
		true => args.value_of("min").unwrap().parse().unwrap(),
		false => 1,
	};

	if http {
		let config = HttpConfig {
			//request_log_max_age_millis: 30_000,
			//main_log_max_age_millis: 30_000,
			//stats_log_max_age_millis: 30_000,
			debug: true,
			..Default::default()
		};
		let mut http_server: HttpServer = HttpServer::new(config);
		http_server.start()?;
		http_server.add_api_mapping("/rustlet".to_string())?;
		http_server.add_api_extension("rsp".to_string())?;
		std::thread::park();
	} else if client {
		info!("Running client");
		info!("Threads={}", threads);
		info!("Iterations={}", itt);
		info!("Requests per thread per iteration={}", count);
		info!("Request length: Max={},Min={}", max, min);
		info_no_ts!(
			"--------------------------------------------------------------------------------"
		);

		let time = std::time::SystemTime::now();
		let tlat_sum = Arc::new(Mutex::new(0.0));
		let tlat_max = Arc::new(Mutex::new(0));

		for x in 0..itt {
			let mut jhs = vec![];
			for i in 0..threads {
				let id = i.clone();
				let tlat_sum = tlat_sum.clone();
				let tlat_max = tlat_max.clone();
				jhs.push(std::thread::spawn(move || {
					let res =
						client_thread(count, id, tlat_sum.clone(), tlat_max.clone(), min, max);
					match res {
						Ok(_) => {}
						Err(e) => {
							info!("Error in client thread: {}", e.to_string());
							assert!(false);
						}
					}
				}));
			}

			for jh in jhs {
				jh.join().expect("panic in thread");
			}
			info!("Iteration {} complete. ", x + 1);
		}

		let elapsed_millis = time.elapsed().unwrap().as_millis();
		let lat_max = tlat_max.lock().unwrap();
		info_no_ts!(
			"--------------------------------------------------------------------------------"
		);
		info!("Test complete in {} ms", elapsed_millis);
		let tlat = tlat_sum.lock().unwrap();
		let avg_lat = (*tlat) / (1_000_000 * count * threads * itt) as f64;
		//let qps_simple = (1000.0 / avg_lat) * threads as f64;
		let qps = (threads * count * itt * 1000) as f64 / elapsed_millis as f64;
		info!("QPS={}", qps);
		info!("Average latency={}ms", avg_lat,);
		info!("Max latency={}ms", (*lat_max) as f64 / (1_000_000 as f64));
	} else {
		let listener = TcpListener::bind("127.0.0.1:9999")?;
		info!("Listener Started");
		let mut eh = EventHandler::new();

		let buffers: Arc<Mutex<HashMap<u128, Buffer>>> = Arc::new(Mutex::new(HashMap::new()));
		let buffers_clone = buffers.clone();
		let buffers_clone2 = buffers.clone();
		eh.set_on_read(move |buf, len, wh| {
			let mut buffers = buffers_clone2.lock().unwrap();
			let held_buf = &mut buffers.get_mut(&wh.get_connection_id());
			match held_buf {
				Some(held_buf) => {
					copy(
						&buf[0..len],
						&mut held_buf.data[held_buf.len..held_buf.len + len],
					);
					held_buf.len += len;
					if held_buf.len < 5 {
						// not enough data
						Ok(())
					} else {
						let exp_len =
							Cursor::new(&held_buf.data[0..4]).read_u32::<LittleEndian>()?;
						let offt = held_buf.data[4] as u32;
						if exp_len + 5 == held_buf.len as u32 {
							let ret_len = held_buf.len;
							held_buf.len = 0;

							// do assertion for our test
							for i in 0..ret_len - 5 {
								if held_buf.data[i as usize + 5]
									!= ((i + offt as usize) % 128) as u8
								{
									info!("invalid data at index = {}", i + 5);
								}
								assert_eq!(
									held_buf.data[i as usize + 5],
									((i + offt as usize) % 128) as u8
								);
							}

							// special case, we disconnect at this len for testing.
							// client is aware and should do an assertion on disconnect.
							wh.write(&held_buf.data.to_vec(), 0, ret_len, exp_len == 99990)?;
							Ok(())
						} else {
							Ok(())
						}
					}
				}
				None => {
					info!("unexpected none: {}, len = {}", wh.get_connection_id(), len);
					Ok(())
				}
			}
		})?;
		eh.set_on_client_read(move |buf, len, wh| {
			wh.write(&buf.to_vec(), 0, len, false)?;
			Ok(())
		})?;
		eh.set_on_accept(move |connection_id, _wh| {
			let mut buffers = buffers.lock().unwrap();

			buffers.insert(connection_id, Buffer::new());

			Ok(())
		})?;

		eh.set_on_close(move |connection_id| {
			let mut buffers = buffers_clone.lock().unwrap();
			buffers.remove(&connection_id);
			Ok(())
		})?;
		eh.start()?;
		eh.add_tcp_listener(&listener)?;
		std::thread::park();
	}
	Ok(())
}
