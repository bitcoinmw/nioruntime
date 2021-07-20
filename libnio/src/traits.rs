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

/// This trait encapsulates all required of the various supported platforms
/// Currently: Kqueues (BSD/OSX), Epoll (Linux), and CompletionPorts (Windows)
pub trait EventHandler {
	/// Add a raw socket for windows event ports
	#[cfg(target_os = "windows")]
	fn add_socket<T: AsRawSocket + ?Sized>(
		&mut self,
		token: usize,
		socket: &T,
	) -> Result<(), Error>;

	/// Remove a raw socket for windows event ports
	#[cfg(target_os = "windows")]
	fn remove_socket<T: AsRawSocket + ?Sized>(
		&mut self,
		token: usize,
		socket: &T,
	) -> Result<(), Error>;

	/// Add a file desciptor for kqueues or epoll (linux, macos)
	#[cfg(any(unix, macos))]
	fn add_fd(&mut self, token: usize, fd: RawFd) -> Result<(), Error>;

	/// Remove a file descriptor for kqueues or epoll (linux, macos)
	#[cfg(any(unix, macos))]
	fn remove_fd(&mut self, token: usize, fd: RawFd) -> Result<(), Error>;

	fn add_tcp_stream(&mut self, token: usize, stream: TcpStream) -> Result<(), Error> {
		#[cfg(any(unix, macos))]
		self.add_fd(token, stream.as_raw_fd())?;
		#[cfg(target_os = "windows")]
		self.add_socket(token, stream.as_raw_socket())?;
		Ok(())
	}

	fn add_tcp_listener(&mut self, token: usize, listener: TcpListener) -> Result<(), Error> {
		#[cfg(any(unix, macos))]
		self.add_fd(token, listener.as_raw_fd())?;
		#[cfg(target_os = "windows")]
		self.add_socket(token, listener.as_raw_socket())?;
		Ok(())
	}

	fn remove_tcp_stream(&mut self, token: usize, stream: TcpStream) -> Result<(), Error> {
		#[cfg(any(unix, macos))]
		self.remove_fd(token, stream.as_raw_fd())?;
		#[cfg(target_os = "windows")]
		self.remove_socket(token, stream.as_raw_socket())?;
		Ok(())
	}

	fn remove_tcp_listener(&mut self, token: usize, listener: TcpListener) -> Result<(), Error> {
		#[cfg(any(unix, macos))]
		self.remove_fd(token, listener.as_raw_fd())?;
		#[cfg(target_os = "windows")]
		self.remove_socket(token, listener.as_raw_socket())?;
		Ok(())
	}
}
