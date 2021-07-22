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

use crate::error::Error;
use futures::executor::block_on;
use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

pub struct FuturesHolder {
	inner: Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>,
}

pub struct ThreadPool {
	tx: Arc<Mutex<mpsc::Sender<FuturesHolder>>>,
}

impl ThreadPool {
	pub fn new(size: usize) -> Result<Self, Error> {
		let (tx, rx): (mpsc::Sender<FuturesHolder>, mpsc::Receiver<FuturesHolder>) =
			mpsc::channel();
		let rx = Arc::new(Mutex::new(rx));

		for _ in 0..size {
			let rx = rx.clone();
			thread::spawn(move || loop {
				let task = {
					let rx = rx.lock();
					match rx {
						Ok(rx) => match (*rx).recv() {
							Ok(task) => task,
							Err(e) => {
								println!("unexpected error in threadpool: {}", e.to_string());
								continue;
							}
						},
						Err(e) => {
							println!("unexpected error in threadpool: {}", e.to_string());
							continue;
						}
					}
				};

				block_on(task.inner);
			});
		}
		let tx = Arc::new(Mutex::new(tx));
		Ok(ThreadPool { tx })
	}

	pub fn execute<F>(&self, f: F) -> Result<(), Error>
	where
		F: Future<Output = ()> + Send + Sync + 'static,
	{
		let f = FuturesHolder { inner: Box::pin(f) };
		{
			let tx = self.tx.lock().unwrap();
			tx.send(f)?;
		}
		Ok(())
	}
}

#[test]
fn test_thread_pool() -> Result<(), Error> {
	let tp = ThreadPool::new(10).unwrap();
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
			*x += 1;
		})
		.unwrap();
	});

	thread::spawn(move || {
		let tp = tp3.clone();
		let tp = tp.lock().unwrap();
		tp.execute(async move {
			let mut x = x2.lock().unwrap();
			*x += 1;
		})
		.unwrap();
	});

	// wait for executors to complete
	std::thread::sleep(std::time::Duration::from_millis(300));
	let x = x3.lock().unwrap();
	assert_eq!(*x, 3);

	Ok(())
}
