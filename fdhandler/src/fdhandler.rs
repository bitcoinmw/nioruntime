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

use crate::util::threadpool::ThreadPool;
use crate::util::Error;
use nix::sys::socket::accept;
use nix::unistd::close;
use nix::unistd::read;
use std::os::unix::io::RawFd;

pub struct ReadClosureHolder {
	closure: Box<dyn Fn(RawFd, &[u8]) -> ()>,
}

impl ReadClosureHolder {
	pub fn new<F>(f: F) -> Self
	where
		F: Fn(RawFd, &[u8]) -> () + 'static,
	{
		ReadClosureHolder {
			closure: Box::new(f),
		}
	}
}

pub struct AcceptClosureHolder {
	closure: Box<dyn Fn(RawFd) -> ()>,
}

impl AcceptClosureHolder {
	pub fn new<F>(f: F) -> Self
	where
		F: Fn(RawFd) -> () + 'static,
	{
		AcceptClosureHolder {
			closure: Box::new(f),
		}
	}
}

pub struct CloseClosureHolder {
	closure: Box<dyn Fn(RawFd) -> ()>,
}

impl CloseClosureHolder {
	pub fn new<F>(f: F) -> Self
	where
		F: Fn(RawFd) -> () + 'static,
	{
		CloseClosureHolder {
			closure: Box::new(f),
		}
	}
}

pub struct FdHandler {
	read_pool: ThreadPool,
	read_handler: Option<ReadClosureHolder>,
	accept_handler: Option<AcceptClosureHolder>,
	close_handler: Option<CloseClosureHolder>,
}

#[derive(Debug)]
pub enum EventType {
	Read,
	Write,
	Accept,
	Error,
}

pub enum ResultType {
	AcceptedFd,
	CloseFd,
	NoResults,
}

pub struct ProcessEventResult {
	pub result_type: ResultType,
	pub fd: Option<RawFd>,
}

impl ProcessEventResult {
	fn new(result_type: ResultType, fd: Option<RawFd>) -> Result<ProcessEventResult, Error> {
		Ok(ProcessEventResult { result_type, fd })
	}
}

impl FdHandler {
	pub fn new(n_read_workers: usize) -> Result<FdHandler, Error> {
		let read_pool = ThreadPool::new(n_read_workers)?;
		Ok(FdHandler {
			read_pool,
			read_handler: None,
			accept_handler: None,
			close_handler: None,
		})
	}

	pub fn process_fd_event(&self, fd: RawFd, etype: EventType) -> Result<(), Error> {
		self.do_process_fd_event(fd, etype)
	}

	fn do_process_fd_event(&self, fd: RawFd, etype: EventType) -> Result<(), Error> {
		match etype {
			EventType::Accept => {
				let res = accept(fd)?;
				if res > 0 {
					match &self.accept_handler {
						Some(handler) => (handler.closure)(res),
						None => {}
					}
				}
			}
			EventType::Read => {
				//self.read_pool.execute(move || {
				self.process_read(fd);
				//});
			}
			_ => {}
		}
		Ok(())
	}

	pub fn register_on_read<F>(&mut self, handler: F) -> Result<(), Error>
	where
		F: Fn(RawFd, &[u8]) -> () + 'static,
	{
		self.read_handler = Some(ReadClosureHolder::new(handler));
		Ok(())
	}

	pub fn register_on_close<F>(&mut self, handler: F) -> Result<(), Error>
	where
		F: Fn(RawFd) -> () + 'static,
	{
		self.close_handler = Some(CloseClosureHolder::new(handler));
		Ok(())
	}

	pub fn register_on_accept<F>(&mut self, handler: F) -> Result<(), Error>
	where
		F: Fn(RawFd) -> () + 'static,
	{
		self.accept_handler = Some(AcceptClosureHolder::new(handler));
		Ok(())
	}

	fn process_read(&self, fd: RawFd) -> Result<(), Error> {
		// first try to read if we can without blocking
		let mut buf = [0u8; 100];
		let res = read(fd, &mut buf);
		match res {
			Ok(len) => {
				match len {
					len if len <= 0 => {
						// end of stream or error
						// need to close and let upstream know
						let _ = close(fd);
						match &self.close_handler {
							Some(handler) => (handler.closure)(fd),
							None => {}
						}
					}
					len if len > 0 => match &self.read_handler {
						Some(handler) => (handler.closure)(fd, &buf[0..len]),
						None => {}
					},
					_ => {}
				}
			}
			Err(e) => {
				println!("read on fd = {} resulted in error: {}", fd, e.to_string());
				// close
				let _ = close(fd);
				println!("close complete");
				match &self.close_handler {
					Some(handler) => (handler.closure)(fd),
					None => {}
				}
			}
		}

		Ok(())
	}
}
