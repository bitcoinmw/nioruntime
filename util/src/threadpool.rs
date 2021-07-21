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
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

pub struct ClosureHolder {
	closure: Box<dyn Fn() -> ()>,
}

impl ClosureHolder {
	pub fn new<F>(f: F) -> Self
	where
		F: Fn() -> () + 'static,
	{
		ClosureHolder {
			closure: Box::new(f),
		}
	}
}

unsafe impl Send for ClosureHolder {}

pub struct ThreadPool {
	tx: mpsc::Sender<ClosureHolder>,
}

impl ThreadPool {
	pub fn new(size: usize) -> Result<Self, Error> {
		let (tx, rx): (mpsc::Sender<ClosureHolder>, mpsc::Receiver<ClosureHolder>) =
			mpsc::channel();
		let rx = Arc::new(Mutex::new(rx));

		for _ in 0..size {
			let rx = rx.clone();
			thread::spawn(move || loop {
				let f = {
					let rx = rx.lock();
					match rx {
						Ok(rx) => match (*rx).recv() {
							Ok(closure) => closure,
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

				(f.closure)();
			});
		}
		Ok(ThreadPool { tx })
	}

	pub fn execute<F>(&self, f: F) -> Result<(), Error>
	where
		F: Fn() -> () + 'static,
	{
		let c = ClosureHolder::new(f);
		self.tx.send(c)?;
		Ok(())
	}
}

#[test]
fn test_thread_pool() -> Result<(), Error> {
	let tp = ThreadPool::new(10).unwrap();
	let x = Arc::new(Mutex::new(0));
	let x1 = x.clone();
	let x2 = x.clone();
	let x3 = x.clone();

	tp.execute(move || {
		let mut x = x.lock().unwrap();
		*x += 1;
	})?;

	tp.execute(move || {
		let mut x = x1.lock().unwrap();
		*x += 1;
	})?;

	tp.execute(move || {
		let mut x = x2.lock().unwrap();
		*x += 1;
	})?;

	// wait for executors to complete
	std::thread::sleep(std::time::Duration::from_millis(300));
	let x = x3.lock().unwrap();
	assert_eq!(*x, 3);

	Ok(())
}
