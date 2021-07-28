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

use byte_tools::copy;
use byteorder::{LittleEndian, ReadBytesExt};
use clap::load_yaml;
use clap::App;
use log::*;
use nioruntime_evh::eventhandler::EventHandler;
use nioruntime_util::{Error, ErrorKind};
use nix::unistd::close;
use rand::Rng;
use std::collections::HashMap;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::os::unix::io::AsRawFd;
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
		Err(e) => println!("real_main generated Error: {}", e.to_string()),
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
	let mut stream = TcpStream::connect("127.0.0.1:9999")?;
	let buf = &mut [0u8; MAX_BUF];
	let buf2 = &mut [0u8; MAX_BUF];
	let start_itt = std::time::SystemTime::now();
	for i in 0..count {
		if i != 0 && i % 10000 == 0 {
			let elapsed = start_itt.elapsed().unwrap().as_millis();
			let qps = (i as f64 / elapsed as f64) * 1000 as f64;
			log!("Request {} on thread {}, qps={}", i, id, qps);
		}
		let start_query = std::time::SystemTime::now();
		let num: u32 = rand::thread_rng().gen_range(min..max);
		let num_buf = num.to_le_bytes();
		copy(&num_buf[0..4], &mut buf[0..4]);

		for i in 0..num {
			buf[i as usize + 4] = (i % 128) as u8;
		}

		let res = stream.write(&buf[0..(num as usize + 4)]);

		match res {
			Ok(_x) => {}
			Err(e) => {
				println!("Error: {}", e.to_string());
				std::thread::sleep(std::time::Duration::from_millis(1));
			}
		}

		let mut len_sum = 0;
		loop {
			let len = stream.read(&mut buf2[len_sum..])?;
			len_sum += len;
			if len_sum == num as usize + 4 {
				break;
			}
		}

		let elapsed = start_query.elapsed().unwrap().as_nanos();
		lat_sum += elapsed as f64;
		if elapsed > lat_max {
			lat_max = elapsed;
		}

		assert_eq!(len_sum, num as usize + 4);
		assert_eq!(Cursor::new(&buf2[0..4]).read_u32::<LittleEndian>()?, num);
		for i in 0..num {
			if buf2[i as usize + 4] != (i % 128) as u8 {
				log!("assertion at {} fails", i);
			}
			assert_eq!(buf2[i as usize + 4], (i % 128) as u8);
		}
	}

	close(stream.as_raw_fd())?;

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
	log_config!(log::LogConfig::default())?;

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

	if client {
		log!("Running client");
		log!("Threads={}", threads);
		log!("Iterations={}", itt);
		log!("Requests per thread per iteration={}", count);
		log!("Request length: Max={},Min={}", max, min);
		log_no_ts!(
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
						Err(e) => println!("Error in client thread: {}", e.to_string()),
					}
				}));
			}

			for jh in jhs {
				jh.join().expect("panic in thread");
			}
			log!("Iteration {} complete. ", x + 1);
		}

		let elapsed_millis = time.elapsed().unwrap().as_millis();
		let lat_max = tlat_max.lock().unwrap();
		log_no_ts!(
			"--------------------------------------------------------------------------------"
		);
		log!("Test complete in {} ms", elapsed_millis);
		let tlat = tlat_sum.lock().unwrap();
		let avg_lat = (*tlat) / (1_000_000 * count * threads * itt) as f64;
		let qps_simple = (1000.0 / avg_lat) * threads as f64;
		log!("QPS={}", qps_simple);
		log!("Average latency={}ms", avg_lat,);
		log!("Max latency={}ms", (*lat_max) as f64 / (1_000_000 as f64));
	} else {
		let listener = TcpListener::bind("127.0.0.1:9999")?;
		log!("Listener Started");
		let mut eh = EventHandler::new();

		let buffers: Arc<Mutex<HashMap<u128, Buffer>>> = Arc::new(Mutex::new(HashMap::new()));
		let buffers_clone = buffers.clone();
		let buffers_clone2 = buffers.clone();
		eh.set_on_read(move |connection_id, _message_id, buf, len| {
			let mut buffers = buffers_clone2.lock().unwrap();
			let held_buf = &mut buffers.get_mut(&connection_id).unwrap();
			copy(
				&buf[0..len],
				&mut held_buf.data[held_buf.len..held_buf.len + len],
			);
			held_buf.len += len;
			if held_buf.len < 4 {
				// not enough data
				Ok((buf.to_vec(), 0, 0))
			} else {
				let exp_len = Cursor::new(&held_buf.data[0..4]).read_u32::<LittleEndian>()?;
				if exp_len + 4 == held_buf.len as u32 {
					Ok((held_buf.data.to_vec(), 0, held_buf.len))
				} else {
					Ok((buf.to_vec(), 0, 0))
				}
			}
		})?;
		eh.set_on_client_read(move |_connection_id, _message_id, buf, len| {
			Ok((buf.to_vec(), 0, len))
		})?;
		eh.set_on_accept(move |connection_id| {
			log!("accept {}", connection_id);

			let mut buffers = buffers.lock().unwrap();
			buffers.insert(connection_id, Buffer::new());

			Ok(())
		})?;

		eh.set_on_close(move |connection_id| {
			log!("close {}", connection_id);
			let mut buffers = buffers_clone.lock().unwrap();
			buffers.remove(&connection_id);
			Ok(())
		})?;
		eh.set_on_write_success(move |_connection_id, _message_id| Ok(()))?;
		eh.set_on_write_fail(move |connection_id, message_id| {
			log!("message fail for cid={},mid={}", connection_id, message_id);
			Ok(())
		})?;
		eh.start()?;
		eh.add_tcp_listener(&listener)?;
		std::thread::park();
	}
	Ok(())
}
