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

use clap::load_yaml;
use clap::App;
use lazy_static::lazy_static;
use nioruntime_kqueues::kqueues::KqueueEventHandler;
use nioruntime_util::Log;
use nioruntime_util::{Error, ErrorKind};
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;

lazy_static! {
	static ref LOG: Arc<Mutex<Log>> = Arc::new(Mutex::new(Log::new()));
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

fn client_thread(count: usize, id: usize, log: &LOG, time: SystemTime) -> Result<(), Error> {
	let mut stream = TcpStream::connect("127.0.0.1:9999")?;
	let buf = &mut [0; 128];
	for i in 0..count {
		if i % 10000 == 0 {
			let mut log = log.lock().map_err(|e| {
				let error: Error = ErrorKind::PoisonError(format!("{}", e)).into();
				error
			})?;
			let elapsed = time.elapsed().unwrap().as_millis();
			let qps = (i as f64 / elapsed as f64) * 1000 as f64;
			log.log(&format!("iteration {} on thread {}, qps={}", i, id, qps))?;
		}
		let res = stream.write(&[1, 2, 3, 4, 5]);
		let len = stream.read(buf)?;
		assert_eq!(len, 5);
		assert_eq!(buf[0], 1);
		assert_eq!(buf[1], 2);
		assert_eq!(buf[2], 3);
		assert_eq!(buf[3], 4);
		assert_eq!(buf[4], 5);
		match res {
			Ok(_) => {}
			Err(e) => {
				println!("Error: {}", e.to_string());
				std::thread::sleep(std::time::Duration::from_millis(1));
			}
		}
	}
	std::thread::sleep(std::time::Duration::from_millis(100));
	println!("thread {} complete", id);
	Ok(())
}

fn real_main() -> Result<(), Error> {
	let log = &LOG;
	{
		let mut log = log.lock().map_err(|e| {
			let error: Error = ErrorKind::PoisonError(format!("{}", e)).into();
			error
		})?;
		log.config(
			None,
			10 * 1024 * 1024, // 10mb
			60 * 60 * 1000,   // 1hr
			true,
			"Log - nioruntime\n\
------------------------------------------------------------------------------",
		)?;
	}
	let yml = load_yaml!("nio.yml");
	let args = App::from_yaml(yml)
		.version(built_info::PKG_VERSION)
		.get_matches();
	let client = args.is_present("client");
	let threads = args.is_present("threads");
	let count = args.is_present("count");

	let threads = match threads {
		true => args.value_of("threads").unwrap().parse().unwrap(),
		false => 1,
	};

	let count = match count {
		true => args.value_of("count").unwrap().parse().unwrap(),
		false => 1,
	};

	if client {
		println!("running client");
		println!("threads={}", threads);
		println!("count={}", count);

		let mut jhs = vec![];
		let time = std::time::SystemTime::now();
		for i in 0..threads {
			let id = i.clone();

			jhs.push(std::thread::spawn(move || {
				let res = client_thread(count, id, log, time);
				match res {
					Ok(_) => {}
					Err(e) => println!("Error in client thread: {}", e.to_string()),
				}
			}));
		}

		for jh in jhs {
			jh.join().expect("panic in thread");
		}
		println!("complete at={}", time.elapsed().unwrap().as_millis());
	} else {
		{
			let mut log = log.lock().map_err(|e| {
				let error: Error = ErrorKind::PoisonError(format!("{}", e)).into();
				error
			})?;
			log.log("Starting listener")?;
		}
		let listener = TcpListener::bind("127.0.0.1:9999")?;
		let mut kqe = KqueueEventHandler::new();
		kqe.set_on_read(move |_connection_id, _message_id, buf, len| {
			//println!("on read {}", _connection_id);
			(buf, 0, len)
		})?;
		kqe.set_on_client_read(move |_connection_id, _message_id, buf, len| (buf, 0, len))?;
		kqe.set_on_accept(move |_connection_id| {})?;
		kqe.set_on_close(move |connection_id| {
			let mut log = log.lock().unwrap();
			log.log(&format!("=====================close {}", connection_id))
				.unwrap();
		})?;
		kqe.set_on_write_success(move |_connection_id, _message_id| {})?;
		kqe.set_on_write_fail(move |connection_id, message_id| {
			println!("message fail for cid={},mid={}", connection_id, message_id);
		})?;
		kqe.start()?;
		kqe.add_tcp_listener(&listener)?;
		std::thread::park();
	}
	Ok(())
}
