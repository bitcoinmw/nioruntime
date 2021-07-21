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

use libc::timespec;
use std::time::Duration;

pub mod kqueues;

pub use nioruntime_util as util;

// Some needed timespec code

#[cfg(not(all(target_os = "freebsd", target_arch = "x86")))]
pub(crate) fn duration_to_timespec(d: Duration) -> timespec {
	let tv_sec = d.as_secs() as i64;
	let tv_nsec = d.subsec_nanos() as i64;

	if tv_sec.is_negative() {
		panic!("Duration seconds is negative");
	}

	if tv_nsec.is_negative() {
		panic!("Duration nsecs is negative");
	}

	timespec { tv_sec, tv_nsec }
}

#[cfg(all(target_os = "freebsd", target_arch = "x86"))]
pub(crate) fn duration_to_timespec(d: Duration) -> timespec {
	let tv_sec = d.as_secs() as i32;
	let tv_nsec = d.subsec_nanos() as i32;

	if tv_sec.is_negative() {
		panic!("Duration seconds is negative");
	}

	if tv_nsec.is_negative() {
		panic!("Duration nsecs is negative");
	}

	timespec { tv_sec, tv_nsec }
}
