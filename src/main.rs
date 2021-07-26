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
use nioruntime_kqueues::kqueues::KqueueEventHandler;
use nioruntime_util::*;
use nix::unistd::close;
use std::io::Read;
use std::io::Write;
use std::net::TcpListener;
use std::net::TcpStream;
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::sync::Mutex;

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
	//log: &LOG,
	tlat_sum: Arc<Mutex<f64>>,
	tlat_max: Arc<Mutex<u128>>,
	complete_waiter: Arc<Mutex<u64>>,
	thread_count: u64,
) -> Result<(), Error> {
	let mut lat_sum = 0.0;
	let mut lat_max = 0;
	let mut stream = TcpStream::connect("127.0.0.1:9999")?;
	let buf = &mut [0; 128];
	let start_itt = std::time::SystemTime::now();
	for i in 0..count {
		if i != 0 && i % 10000 == 0 {
			/*
			let mut log = log.lock().map_err(|e| {
				let error: Error = ErrorKind::PoisonError(format!("{}", e)).into();
				error
			})?; */
			let elapsed = start_itt.elapsed().unwrap().as_millis();
			let qps = (i as f64 / elapsed as f64) * 1000 as f64;
			//log.log(&format!("Request {} on thread {}, qps={}", i, id, qps))?;
			log!("Request {} on thread {}, qps={}", i, id, qps);
		}
		let start_query = std::time::SystemTime::now();
		let res = stream.write(&[1, 2, 3, 4, 5]);
		let len = stream.read(buf)?;
		let elapsed = start_query.elapsed().unwrap().as_nanos();
		lat_sum += elapsed as f64;
		if elapsed > lat_max {
			lat_max = elapsed;
		}

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
	{
		let mut complete_waiter = complete_waiter.lock().unwrap();
		(*complete_waiter) += 1;
	}

	loop {
		let complete_waiter = complete_waiter.lock().unwrap();
		if (*complete_waiter) == thread_count {
			let fd = stream.as_raw_fd();
			close(fd)?;
			break;
		}
	}
	std::thread::sleep(std::time::Duration::from_millis(10));
	{
		let mut complete_waiter = complete_waiter.lock().unwrap();
		(*complete_waiter) -= 1;
	}

	loop {
		let complete_waiter = complete_waiter.lock().unwrap();
		if (*complete_waiter) == 0 {
			break;
		}
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
	log_config!(nioruntime_util::LogConfig::default())?;
	{
		//log_config!(nioruntime_util::LogConfig::default())?;
		//log!("ok {} {} x={:?}", 1, "second str", "hi");
	}
	/*
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
				"",
				true,
			)?;
		}
	*/
	let yml = load_yaml!("nio.yml");
	let args = App::from_yaml(yml)
		.version(built_info::PKG_VERSION)
		.get_matches();
	let client = args.is_present("client");
	let threads = args.is_present("threads");
	let count = args.is_present("count");
	let itt = args.is_present("itt");

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

	if client {
		log!("running client");
		log!("threads={}", threads);
		log!("iterations={}", itt);
		log!("count={}", count);
		/*
				{
					let mut log = log.lock().map_err(|e| {
						let error: Error = ErrorKind::PoisonError(format!("{}", e)).into();
						error
					})?;

					log.log(&format!("running client"))?;
					log.log(&format!("threads={}", threads))?;
					log.log(&format!("iterations={}", itt))?;
					log.log(&format!("count={}", count))?;
				}
		*/

		let time = std::time::SystemTime::now();
		let tlat_sum = Arc::new(Mutex::new(0.0));
		let tlat_max = Arc::new(Mutex::new(0));

		for x in 0..itt {
			let mut jhs = vec![];
			let complete_waiter = Arc::new(Mutex::new(0));
			for i in 0..threads {
				let id = i.clone();
				let tlat_sum = tlat_sum.clone();
				let tlat_max = tlat_max.clone();
				let complete_waiter = complete_waiter.clone();
				jhs.push(std::thread::spawn(move || {
					let res = client_thread(
						count,
						id,
						/*log,*/
						tlat_sum.clone(),
						tlat_max.clone(),
						complete_waiter,
						threads as u64,
					);
					match res {
						Ok(_) => {}
						Err(e) => println!("Error in client thread: {}", e.to_string()),
					}
				}));
			}

			for jh in jhs {
				jh.join().expect("panic in thread");
			}
			/*
						{
							let mut log = log.lock().map_err(|e| {
												let error: Error = ErrorKind::PoisonError(format!("{}", e)).into();
												error
										})?;
							log.log(&format!("Iteration {} complete. ", x+1))?;
						}
			*/
			log!("Iteration {} complete. ", x + 1);
			std::thread::sleep(std::time::Duration::from_millis(100));
		}
		{
			/*
						let mut log = log.lock().map_err(|e| {
							let error: Error = ErrorKind::PoisonError(format!("{}", e)).into();
							error
						})?;
			*/
			let elapsed_millis = time.elapsed().unwrap().as_millis();
			let lat_max = tlat_max.lock().unwrap();
			//			log.log(&format!("Complete at={} ms", elapsed_millis))?;
			log!("Complete at={} ms", elapsed_millis);
			let total_qps = 1000 as f64
				* (count as f64 * threads as f64 * itt as f64
					/ (elapsed_millis - (itt as u128) * 200) as f64);
			//log.log(&format!("Total QPS={}", total_qps))?;
			log!("Total QPS={}", total_qps);
			let tlat = tlat_sum.lock().unwrap();
			log!(
				"Average latency={}ms",
				(*tlat) / (1_000_000 * count * threads * itt) as f64
			);
			log!("Max latency={}ms", (*lat_max) as f64 / (1_000_000 as f64));
			/*
						log.log(&format!(
							"Average latency={}ms",
							(*tlat) / (1_000_000 * count * threads * itt) as f64
						))?;
						log.log(&format!(
							"Max latency={}ms",
							(*lat_max) as f64 / (1_000_000 as f64),
						))?;
			*/
		}
	} else {
		{
			/*
						let mut log = log.lock().map_err(|e| {
							let error: Error = ErrorKind::PoisonError(format!("{}", e)).into();
							error
						})?;
						log.log("Starting listener")?;
			*/
			log!("Starting listener{}", "");
		}
		let listener = TcpListener::bind("127.0.0.1:9999")?;
		let mut kqe = KqueueEventHandler::new();
		kqe.set_on_read(move |_connection_id, _message_id, buf, len| Ok((buf, 0, len)))?;
		kqe.set_on_client_read(move |_connection_id, _message_id, buf, len| Ok((buf, 0, len)))?;
		kqe.set_on_accept(move |_connection_id| {
			log!("=====================accept {}", _connection_id);
			/*
									let log = log.lock();
									let res = match log {
											Ok(mut log) => log.log(&format!("=====================accept {}", _connection_id)),
											Err(e) => Err(ErrorKind::PoisonError(format!(
													"Logging gerneated poison error: {}",
													e.to_string()
											))
											.into()),
									};
									match res {
											Ok(_) => {}
											Err(e) => println!(
													"Logging generated error: {}\nAttempted Log message: {}",
													e.to_string(),
													format!("=====================accept {}", _connection_id),
											),
									}
			*/
			Ok(())
		})?;
		kqe.set_on_close(move |connection_id| {
			log!("=====================close {}", connection_id);
			/*
						let log = log.lock();
						let res = match log {
							Ok(mut log) => log.log(&format!("=====================close {}", connection_id)),
							Err(e) => Err(ErrorKind::PoisonError(format!(
								"Logging gerneated poison error: {}",
								e.to_string()
							))
							.into()),
						};
						match res {
							Ok(_) => {}
							Err(e) => println!(
								"Logging generated error: {}\nAttempted Log message: {}",
								e.to_string(),
								format!("=====================close {}", connection_id),
							),
						}
			*/
			Ok(())
		})?;
		kqe.set_on_write_success(move |_connection_id, _message_id| Ok(()))?;
		kqe.set_on_write_fail(move |connection_id, message_id| {
			log!("message fail for cid={},mid={}", connection_id, message_id);
			/*
						let log = log.lock();
						let res = match log {
							Ok(mut log) => log.log(&format!(
								"message fail for cid={},mid={}",
								connection_id, message_id
							)),
							Err(e) => Err(ErrorKind::PoisonError(format!(
								"Logging gerneated poison error: {}",
								e.to_string()
							))
							.into()),
						};
						match res {
							Ok(_) => {}
							Err(e) => println!(
								"Logging generated error: {}\nAttempted Log message: {}",
								e.to_string(),
								&format!("message fail for cid={},mid={}", connection_id, message_id)
							),
						}
			*/
			Ok(())
		})?;
		kqe.start()?;
		kqe.add_tcp_listener(&listener)?;
		std::thread::park();
	}
	Ok(())
}
