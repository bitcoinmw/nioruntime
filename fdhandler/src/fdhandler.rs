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

use crate::util::Error;
use nix::sys::socket::accept;
use nix::unistd::close;
use nix::unistd::read;
use std::os::unix::io::RawFd;
use threadpool::ThreadPool;

pub struct FdHandler {
	read_pool: ThreadPool,
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
		let read_pool = ThreadPool::new(n_read_workers);
		Ok(FdHandler { read_pool })
	}

	pub fn process_fd_event(
		&self,
		fd: RawFd,
		etype: EventType,
	) -> Result<ProcessEventResult, Error> {
		self.do_process_fd_event(fd, etype)
	}

	fn do_process_fd_event(
		&self,
		fd: RawFd,
		etype: EventType,
	) -> Result<ProcessEventResult, Error> {
		println!("process fd event: {}, type: {:?}", fd, etype);
		match etype {
			EventType::Accept => {
				let res = accept(fd)?;
				Ok(ProcessEventResult::new(ResultType::AcceptedFd, Some(res))?)
			}
			EventType::Read => {
				let mut buf = [0u8; 100];
				let res = read(fd, &mut buf);
				match res {
					Ok(len) => {
						match len {
							len if len <= 0 => {
								// end of stream or error
								// need to close and let upstream know
								println!("closing connection {}", fd);
								close(fd)?;
								Ok(ProcessEventResult::new(ResultType::CloseFd, Some(fd))?)
							}
							len if len > 0 => {
								println!(
									"read {} bytes msg = {}",
									len,
									std::str::from_utf8(&buf[0..len])?
								);
								Ok(ProcessEventResult::new(ResultType::NoResults, None)?)
							}
							_ => Ok(ProcessEventResult::new(ResultType::NoResults, None)?), // for compiler
						}
					}
					Err(e) => {
						println!("read on fd = {} resulted in error: {}", fd, e.to_string());
						// close
						close(fd)?;
						Ok(ProcessEventResult::new(ResultType::CloseFd, Some(fd))?)
					}
				}
			}
			_ => Ok(ProcessEventResult::new(ResultType::NoResults, None)?),
		}
	}

	pub fn register_handler(&self, handler: fn(i32) -> i32) -> Result<(), Error> {
		Ok(())
	}
}
