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

use crate::error::{Error, ErrorKind};
use futures::executor::block_on;
use lazy_static::lazy_static;
use rand;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

lazy_static! {
	pub static ref STATIC_THREAD_POOL: Arc<Mutex<HashMap<u128, ThreadPoolImpl>>> =
		Arc::new(Mutex::new(HashMap::new()));
}

pub struct StaticThreadPool {
	id: u128,
}

impl StaticThreadPool {
	pub fn new() -> Result<Self, Error> {
		let tp = ThreadPoolImpl::new();
		let id: u128 = rand::random::<u128>();
		let mut stp = STATIC_THREAD_POOL.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!(
				"static thread pool lock error: {}",
				e.to_string()
			))
			.into();
			error
		})?;
		stp.insert(id, tp);

		Ok(StaticThreadPool { id })
	}

	pub fn start(&self, size: usize) -> Result<(), Error> {
		let mut stp = STATIC_THREAD_POOL.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!(
				"static thread pool lock error: {}",
				e.to_string()
			))
			.into();
			error
		})?;

		let tp = stp.get_mut(&self.id);
		match tp {
			Some(tp) => tp.start(size)?,
			None => {
				return Err(ErrorKind::InternalError(format!(
					"static thread pool id = {} doesn't exist error",
					self.id
				))
				.into())
			}
		}

		Ok(())
	}

	pub fn stop(&self) -> Result<(), Error> {
		let stp = STATIC_THREAD_POOL.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!(
				"static thread pool lock error: {}",
				e.to_string()
			))
			.into();
			error
		})?;

		let tp = stp.get(&self.id);
		match tp {
			Some(tp) => tp.stop()?,
			None => {
				return Err(ErrorKind::InternalError(format!(
					"static thread pool id = {} doesn't exist error",
					self.id
				))
				.into())
			}
		}
		Ok(())
	}

	pub fn execute<F>(&self, f: F) -> Result<(), Error>
	where
		F: Future<Output = ()> + Send + Sync + 'static,
	{
		let stp = STATIC_THREAD_POOL.lock().map_err(|e| {
			let error: Error = ErrorKind::InternalError(format!(
				"static thread pool lock error: {}",
				e.to_string()
			))
			.into();
			error
		})?;

		let tp = stp.get(&self.id);
		match tp {
			Some(tp) => tp.execute(f)?,
			None => {
				return Err(ErrorKind::InternalError(format!(
					"static thread pool id = {} doesn't exist error",
					self.id
				))
				.into())
			}
		}
		Ok(())
	}
}

pub struct FuturesHolder {
	inner: Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>,
}

pub struct ThreadPoolImpl {
	tx: Arc<Mutex<mpsc::Sender<(FuturesHolder, bool)>>>,
	rx: Arc<Mutex<mpsc::Receiver<(FuturesHolder, bool)>>>,
	size: Arc<Mutex<usize>>,
}

impl ThreadPoolImpl {
	pub fn new() -> Self {
		let (tx, rx): (
			mpsc::Sender<(FuturesHolder, bool)>,
			mpsc::Receiver<(FuturesHolder, bool)>,
		) = mpsc::channel();
		let rx = Arc::new(Mutex::new(rx));
		let tx = Arc::new(Mutex::new(tx));
		ThreadPoolImpl {
			tx,
			rx,
			size: Arc::new(Mutex::new(0)),
		}
	}

	pub fn start(&mut self, size: usize) -> Result<(), Error> {
		{
			let mut self_size = self.size.lock().map_err(|_e| {
				let error: Error = ErrorKind::PoisonError("size lock".to_string()).into();
				error
			})?;
			(*self_size) = size;
		}
		for _id in 0..size {
			let rx = self.rx.clone();
			thread::spawn(move || loop {
				let rx = rx.clone();
				let jh = thread::spawn(move || loop {
					let task = {
						let res = rx.lock();

						match res {
							Ok(rx) => match rx.recv() {
								Ok((task, stop)) => {
									if stop {
										break;
									}
									task
								}
								Err(e) => {
									println!("unexpected error in threadpool: {}", e.to_string());
									std::thread::sleep(std::time::Duration::from_millis(1000));
									break;
								}
							},
							Err(e) => {
								println!("unexpected error in threadpool: {}", e.to_string());
								std::thread::sleep(std::time::Duration::from_millis(1000));
								break;
							}
						}
					};

					block_on(task.inner);
				});
				let _ = jh.join();
			});
		}

		Ok(())
	}

	pub fn stop(&self) -> Result<(), Error> {
		let size = {
			let size = self.size.lock().map_err(|_e| {
				let error: Error = ErrorKind::PoisonError("stop size lock".to_string()).into();
				error
			});
			size
		}?;
		for _ in 0..*size {
			let f = async {};
			let f = FuturesHolder { inner: Box::pin(f) };
			let tx = self.tx.lock().unwrap();
			tx.send((f, true))?;
		}
		Ok(())
	}

	pub fn execute<F>(&self, f: F) -> Result<(), Error>
	where
		F: Future<Output = ()> + Send + Sync + 'static,
	{
		let f = FuturesHolder { inner: Box::pin(f) };
		{
			let tx = self.tx.lock().unwrap();
			tx.send((f, false))?;
		}
		Ok(())
	}
}

#[test]
fn test_thread_pool() -> Result<(), Error> {
	let tp = StaticThreadPool::new()?;
	tp.start(10)?;
	let tp = Arc::new(Mutex::new(tp));
	let x = Arc::new(Mutex::new(0));
	let x1 = x.clone();
	let x2 = x.clone();
	let x3 = x.clone();
	let tp1 = tp.clone();
	let tp2 = tp.clone();
	let tp3 = tp.clone();

	thread::spawn(move || {
		let tp = tp1.clone();
		let tp = tp.lock().unwrap();
		tp.execute(async move {
			let mut x = x.lock().unwrap();
			*x += 1;
		})
		.unwrap();
	});

	thread::spawn(move || {
		let tp = tp2.clone();
		let tp = tp.lock().unwrap();
		tp.execute(async move {
			let mut x = x1.lock().unwrap();
			*x += 2;
		})
		.unwrap();
	});

	thread::spawn(move || {
		let tp = tp3.clone();
		let tp = tp.lock().unwrap();
		tp.execute(async move {
			let mut x = x2.lock().unwrap();
			*x += 3;
		})
		.unwrap();
	});

	// wait for executors to complete
	std::thread::sleep(std::time::Duration::from_millis(300));
	let x = x3.lock().unwrap();
	assert_eq!(*x, 6);

	Ok(())
}

#[test]
fn test_stop_thread_pool() -> Result<(), Error> {
	let tp = StaticThreadPool::new()?;
	tp.start(10)?;
	let tp = Arc::new(Mutex::new(tp));
	let x = Arc::new(Mutex::new(0));
	let x1 = x.clone();
	let x2 = x.clone();
	let x3 = x.clone();
	let tp1 = tp.clone();
	let tp2 = tp.clone();
	let tp3 = tp.clone();
	let tp4 = tp.clone();

	thread::spawn(move || {
		let tp = tp1.clone();
		let tp = tp.lock().unwrap();
		tp.execute(async move {
			let mut x = x.lock().unwrap();
			*x += 1;
		})
		.unwrap();
	});

	thread::spawn(move || {
		let tp = tp2.clone();
		let tp = tp.lock().unwrap();
		tp.execute(async move {
			let mut x = x1.lock().unwrap();
			*x += 2;
		})
		.unwrap();
	});

	std::thread::sleep(std::time::Duration::from_millis(1000));
	let tp4 = tp4.lock().unwrap();
	tp4.stop()?;
	std::thread::sleep(std::time::Duration::from_millis(1000));

	thread::spawn(move || {
		let tp = tp3.clone();
		let tp = tp.lock().unwrap();
		tp.execute(async move {
			let mut x = x2.lock().unwrap();
			*x += 313;
		})
		.unwrap();
	});

	// wait for executors to complete
	std::thread::sleep(std::time::Duration::from_millis(300));
	let x = x3.lock().unwrap();
	assert_eq!(*x, 3);

	Ok(())
}
