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

/// A macro that is used to lock a mutex and return the appropriate error if the lock is poisoned.
/// This code was used in many places, and this macro simplifies it.
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

/// A macro that is used to lock a rwlock in write mode and return the appropriate error if the lock is poisoned.
/// This code was used in many places, and this macro simplifies it.
#[macro_export]
macro_rules! lockw {
	($a:expr) => {
		$a.write().map_err(|e| {
			let error: Error =
				ErrorKind::PoisonError(format!("Poison Error: {}", e.to_string())).into();
			error
		})?;
	};
}

/// A macro that is used to lock a rwlock in read mode and return the appropriate error if the lock is poisoned.
/// This code was used in many places, and this macro simplifies it.
#[macro_export]
macro_rules! lockr {
	($a:expr) => {
		$a.read().map_err(|e| {
			let error: Error =
				ErrorKind::PoisonError(format!("Poison Error: {}", e.to_string())).into();
			error
		})?;
	};
}

/// A macro that is used to lock a rwlock in read mode ignoring poison locks
/// This code was used in many places, and this macro simplifies it.
#[macro_export]
macro_rules! lockrp {
	($a:expr) => {
		match $a.read() {
			Ok(data) => data,
			Err(e) => e.into_inner(),
		}
	};
}

/// A macro that is used to lock a rwlock in write mode ignoring poison locks
/// This code was used in many places, and this macro simplifies it.
#[macro_export]
macro_rules! lockwp {
	($a:expr) => {
		match $a.write() {
			Ok(data) => data,
			Err(e) => e.into_inner(),
		}
	};
}
