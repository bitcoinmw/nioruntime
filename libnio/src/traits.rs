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
use std::net::TcpListener;
use std::net::TcpStream;

#[cfg(target_os = "windows")]
use async_std::os::windows::io::AsRawSocket;

#[cfg(any(unix, macos))]
use std::os::unix::io::{AsRawFd, RawFd};

#[derive(Debug, Clone)]
pub enum ActionType {
	AddStream,
	AddListener,
	Remove,
}

/// This trait encapsulates all required of the various supported platforms
/// Currently: Kqueues (BSD/OSX), Epoll (Linux), and CompletionPorts (Windows)
pub trait EventHandler {
	/// Add a raw socket for windows event ports
	#[cfg(target_os = "windows")]
	fn add_windows_socket<T: AsRawSocket + ?Sized>(
		&mut self,
		socket: &T,
		atype: ActionType,
	) -> Result<i32, Error>;

	/// Add a file desciptor for kqueues or epoll (linux, macos)
	#[cfg(any(unix, macos))]
	fn add_fd(&mut self, fd: RawFd, atype: ActionType) -> Result<i32, Error>;

	/// Remove the given id from the event handler
	fn remove_fd(&mut self, id: i32) -> Result<(), Error>;

	fn add_tcp_stream(&mut self, stream: &TcpStream) -> Result<i32, Error> {
		stream.set_nonblocking(true)?;
		#[cfg(any(unix, macos))]
		let ret = self.add_fd(stream.as_raw_fd(), ActionType::AddStream)?;
		#[cfg(target_os = "windows")]
		let ret = self.add_socket(stream.as_raw_socket(), ActionType::AddStream)?;
		Ok(ret)
	}

	fn add_tcp_listener(&mut self, listener: &TcpListener) -> Result<i32, Error> {
		// must be nonblocking
		listener.set_nonblocking(true)?;
		#[cfg(any(unix, macos))]
		let ret = self.add_fd(listener.as_raw_fd(), ActionType::AddListener)?;
		#[cfg(target_os = "windows")]
		let ret = self.add_socket(listener.as_raw_socket(), ActionType::AddListener)?;
		Ok(ret)
	}

	fn remove_tcp_stream(&mut self, stream: TcpStream) -> Result<(), Error> {
		#[cfg(any(unix, macos))]
		self.remove_fd(stream.as_raw_fd())?;
		#[cfg(target_os = "windows")]
		self.remove_socket(stream.as_raw_socket())?;
		Ok(())
	}

	fn remove_tcp_listener(&mut self, listener: TcpListener) -> Result<(), Error> {
		#[cfg(any(unix, macos))]
		self.remove_fd(listener.as_raw_fd())?;
		#[cfg(target_os = "windows")]
		self.remove_socket(listener.as_raw_socket())?;
		Ok(())
	}
}
