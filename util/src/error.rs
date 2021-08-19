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

use crate::threadpool::FuturesHolder;
use failure::{Backtrace, Context, Fail};
#[cfg(unix)]
use nix::errno::Errno;
use std::ffi::OsString;
use std::fmt;
use std::fmt::Display;
use std::str::Utf8Error;

#[macro_export]
macro_rules! lock {
	($a:expr) => {
		$a.lock().map_err(|e| {
			let error: Error =
				ErrorKind::PoisonError(format!("Poison Error: {}", e.to_string())).into();
			error
		})?;
	};
}

/// Base Error struct which is used throught this crate and other crates
#[derive(Debug, Fail)]
pub struct Error {
	inner: Context<ErrorKind>,
}

/// Kinds of errors that can occur
#[derive(Clone, Eq, PartialEq, Debug, Fail)]
pub enum ErrorKind {
	/// IOError Error
	#[fail(display = "IOError Error: {}", _0)]
	IOError(String),
	/// Send Error
	#[fail(display = "Send Error: {}", _0)]
	SendError(String),
	/// Internal Error
	#[fail(display = "Internal Error: {}", _0)]
	InternalError(String),
	/// Stale Fd
	#[fail(display = "Stale Fd Error: {}", _0)]
	StaleFdError(String),
	/// Array Index out of bounds
	#[fail(display = "ArrayIndexOutofBounds: {}", _0)]
	ArrayIndexOutofBounds(String),
	/// Setup Error
	#[fail(display = "Setup Error: {}", _0)]
	SetupError(String),
	/// Log not configured
	#[fail(display = "Log not configured Error: {}", _0)]
	LogNotConfigured(String),
	/// OsString error
	#[fail(display = "OsString Error: {}", _0)]
	OsStringError(String),
	/// Poison error multiple locks
	#[fail(display = "Poison Error: {}", _0)]
	PoisonError(String),
	/// Connection close
	#[fail(display = "Connection Close Error: {}", _0)]
	ConnectionCloseError(String),
	/// Ordering Error
	#[fail(display = "Ordering Error: {}", _0)]
	OrderingError(String),
	/// Invalid RSP (Rust Server Page)
	#[fail(display = "Invalid RSP Error: {}", _0)]
	InvalidRSPError(String),
}

impl Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let cause = match self.cause() {
			Some(c) => format!("{}", c),
			None => String::from("Unknown"),
		};
		let backtrace = match self.backtrace() {
			Some(b) => format!("{}", b),
			None => String::from("Unknown"),
		};
		let output = format!(
			"{} \n Cause: {} \n Backtrace: {}",
			self.inner, cause, backtrace
		);
		Display::fmt(&output, f)
	}
}

impl Error {
	/// get kind
	pub fn kind(&self) -> ErrorKind {
		self.inner.get_context().clone()
	}
	/// get cause
	pub fn cause(&self) -> Option<&dyn Fail> {
		self.inner.cause()
	}
	/// get backtrace
	pub fn backtrace(&self) -> Option<&Backtrace> {
		self.inner.backtrace()
	}
}

impl From<ErrorKind> for Error {
	fn from(kind: ErrorKind) -> Error {
		Error {
			inner: Context::new(kind),
		}
	}
}

impl From<std::io::Error> for Error {
	fn from(e: std::io::Error) -> Error {
		Error {
			inner: Context::new(ErrorKind::IOError(format!("{}", e))),
		}
	}
}

#[cfg(unix)]
impl From<Errno> for Error {
	fn from(e: Errno) -> Error {
		Error {
			inner: Context::new(ErrorKind::IOError(format!("{}", e))),
		}
	}
}

impl From<Utf8Error> for Error {
	fn from(e: Utf8Error) -> Error {
		Error {
			inner: Context::new(ErrorKind::IOError(format!("{}", e))),
		}
	}
}

impl From<std::sync::mpsc::SendError<(FuturesHolder, bool)>> for Error {
	fn from(e: std::sync::mpsc::SendError<(FuturesHolder, bool)>) -> Error {
		Error {
			inner: Context::new(ErrorKind::IOError(format!("{}", e))),
		}
	}
}

impl From<OsString> for Error {
	fn from(e: OsString) -> Error {
		Error {
			inner: Context::new(ErrorKind::OsStringError(format!("{:?}", e))),
		}
	}
}
