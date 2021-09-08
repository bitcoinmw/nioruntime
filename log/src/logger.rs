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

#![macro_use]

//! A logging library.

use chrono::{DateTime, Local, Utc};
use lazy_static::lazy_static;
use nioruntime_err::{Error, ErrorKind};
use std::collections::HashMap;
use std::fs::{canonicalize, metadata, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

pub const TRACE: i32 = 0;
pub const DEBUG: i32 = 1;
pub const INFO: i32 = 2;
pub const WARN: i32 = 3;
pub const ERROR: i32 = 4;
pub const FATAL: i32 = 5;

lazy_static! {
	/// This is the static holder of all log objects. Generally this
	/// should not be called directly. See [`log`] instead.
	pub static ref STATIC_LOG: Arc<Mutex<HashMap<String, Log>>> = Arc::new(Mutex::new(HashMap::new()));
	static ref START_TIME: Instant = Instant::now();
}

/// Log at the 'fatal' (5) log level. This macro calls the default logger. To configure this
/// logger, see [`log_config`]. It is used like the pritln/format macros.
/// Also see [`trace`] [`debug`], [`info`], [`warn`], or [`error`].
/// # Examples
/// ```
/// use nioruntime_log::*;
/// // log level must be set before calling any logging function.
/// // typically it is done at the top of a file so that it's easy to change.
/// // but it can be done at any level or scope. The inner scope prevails.
/// fatal!(); // set log level to fatal "5"
///
/// let abc = 123;
/// fatal!("my value = {}", abc);
/// fatal!("hi");
///
/// // The output will look like this:
/// // [2021-08-09 19:41:37]: my value = 123
/// // [2021-08-09 19:41:37]: hi
/// ```
#[macro_export]
macro_rules! fatal {
        () => {
                nioruntime_log::do_log!(nioruntime_log::FATAL);
        };
        ($a:expr) => {
                {
                        nioruntime_log::log!(nioruntime_log::FATAL, $a);
                }
        };
        ($a:expr,$($b:tt)*)=>{
                {
                        nioruntime_log::log!(nioruntime_log::FATAL, $a, $($b)*);
                }
        };
}

/// Just like [`fatal`], but with no timestamp.
#[macro_export]
macro_rules! fatal_no_ts {
        ($a:expr) => {
                {
                        nioruntime_log::log_no_ts!(nioruntime_log::FATAL, $a);
                }
        };
        ($a:expr,$($b:tt)*)=>{
                {
                        nioruntime_log::log_no_ts!(nioruntime_log::FATAL, $a, $($b)*);
                }
        };
}

/// Log at the 'error' (4) log level. This macro calls the default logger. To configure this
/// logger, see [`log_config`]. It is used like the pritln/format macros.
/// Also see [`trace`], [`debug`], [`info`], [`warn`], or [`fatal`].
/// # Examples
/// ```
/// use nioruntime_log::*;
/// // log level must be set before calling any logging function.
/// // typically it is done at the top of a file so that it's easy to change.
/// // but it can be done at any level or scope. The inner scope prevails.
/// error!(); // set log level to error "4"
///
/// let abc = 123;
/// error!("my value = {}", abc);
/// error!("hi");
///
/// // The output will look like this:
/// // [2021-08-09 19:41:37]: my value = 123
/// // [2021-08-09 19:41:37]: hi
/// ```
#[macro_export]
macro_rules! error {
        () => {
                nioruntime_log::do_log!(nioruntime_log::ERROR);
        };
        ($a:expr) => {
                {
                        nioruntime_log::log!(nioruntime_log::ERROR, $a);
                }
        };
        ($a:expr,$($b:tt)*)=>{
                {
                        nioruntime_log::log!(nioruntime_log::ERROR, $a, $($b)*);
                }
        };
}

/// Just like [`error`], but with no timestamp.
#[macro_export]
macro_rules! error_no_ts {
        ($a:expr) => {
                {
                        nioruntime_log::log_no_ts!(nioruntime_log::ERROR, $a);
                }
        };
        ($a:expr,$($b:tt)*)=>{
                {
                        nioruntime_log::log_no_ts!(nioruntime_log::ERROR, $a, $($b)*);
                }
        };
}

/// Log at the 'warn' (3) log level. This macro calls the default logger. To configure this
/// logger, see [`log_config`]. It is used like the pritln/format macros.
/// Also see [`trace`], [`debug`], [`info`], [`error`], or [`fatal`].
/// # Examples
/// ```
/// use nioruntime_log::*;
/// // log level must be set before calling any logging function.
/// // typically it is done at the top of a file so that it's easy to change.
/// // but it can be done at any level or scope. The inner scope prevails.
/// warn!(); // set log level to warn "3"
///
/// let abc = 123;
/// warn!("my value = {}", abc);
/// warn!("hi");
///
/// // The output will look like this:
/// // [2021-08-09 19:41:37]: my value = 123
/// // [2021-08-09 19:41:37]: hi
/// ```
#[macro_export]
macro_rules! warn {
        () => {
                nioruntime_log::do_log!(nioruntime_log::WARN);
        };
        ($a:expr) => {
		{
                	nioruntime_log::log!(nioruntime_log::WARN, $a);
		}
        };
        ($a:expr,$($b:tt)*)=>{
                {
                	nioruntime_log::log!(nioruntime_log::WARN, $a, $($b)*);
		}
        };
}

/// Just like [`warn`], but with no timestamp.
#[macro_export]
macro_rules! warn_no_ts {
        ($a:expr) => {
                {
                	nioruntime_log::log_no_ts!(nioruntime_log::WARN, $a);
		}
        };
        ($a:expr,$($b:tt)*)=>{
                {
                	nioruntime_log::log_no_ts!(nioruntime_log::WARN, $a, $($b)*);
		}
        };
}

/// Log at the 'info' (2) log level. This macro calls the default logger. To configure this
/// logger, see [`log_config`]. It is used like the pritln/format macros.
/// Also see [`trace`], [`debug`], [`warn`], [`error`], or [`fatal`].
/// # Examples
/// ```
/// use nioruntime_log::*;
/// // log level must be set before calling any logging function.
/// // typically it is done at the top of a file so that it's easy to change.
/// // but it can be done at any level or scope. The inner scope prevails.
/// info!(); // set log level to info "2"
///
/// let abc = 123;
/// info!("my value = {}", abc);
/// info!("hi");
///
/// // The output will look like this:
/// // [2021-08-09 19:41:37]: my value = 123
/// // [2021-08-09 19:41:37]: hi
/// ```
#[macro_export]
macro_rules! info {
	() => {
		nioruntime_log::do_log!(nioruntime_log::INFO);
	};
        ($a:expr) => {
                {
                	nioruntime_log::log!(nioruntime_log::INFO, $a);
		}
        };
        ($a:expr,$($b:tt)*)=>{
                {
                	nioruntime_log::log!(nioruntime_log::INFO, $a, $($b)*);
		}
        };
}

/// Just like [`info`], but with no timestamp.
#[macro_export]
macro_rules! info_no_ts {
        ($a:expr) => {
                {
                	nioruntime_log::log_no_ts!(nioruntime_log::INFO, $a);
		}
        };
        ($a:expr,$($b:tt)*)=>{
                {
                	nioruntime_log::log_no_ts!(nioruntime_log::INFO, $a, $($b)*);
		}
        };
}

/// Log at the 'debug' (1) log level. This macro calls the default logger. To configure this
/// logger, see [`log_config`]. It is used like the pritln/format macros.
/// Also see [`trace`], [`info`], [`warn`], [`error`], or [`fatal`].
/// # Examples
/// ```
/// use nioruntime_log::*;
/// // log level must be set before calling any logging function.
/// // typically it is done at the top of a file so that it's easy to change.
/// // but it can be done at any level or scope. The inner scope prevails.
/// debug!(); // set log level to debug "1"
///
/// let abc = 123;
/// debug!("my value = {}", abc);
/// debug!("hi");
///
/// // The output will look like this:
/// // [2021-08-09 19:41:37]: my value = 123
/// // [2021-08-09 19:41:37]: hi
/// ```
#[macro_export]
macro_rules! debug {
	() => {
		nioruntime_log::do_log!(nioruntime_log::DEBUG);
	};
        ($a:expr) => {
                {
                	log!(nioruntime_log::DEBUG, $a);
		}
        };
        ($a:expr,$($b:tt)*)=>{
                {
                	log!(nioruntime_log::DEBUG, $a, $($b)*);
		}
        };
}

/// Just like [`debug`], but with no timestamp.
#[macro_export]
macro_rules! debug_no_ts {
        ($a:expr) => {
                {
                	nioruntime_log::log_no_ts!(nioruntime_log::DEBUG, $a);
		}
        };
        ($a:expr,$($b:tt)*)=>{
                {
                	nioruntime_log::log_no_ts!(nioruntime_log::DEBUG, $a, $($b)*);
		}
        };
}

/// Log at the 'trace' (0) log level. This macro calls the default logger. To configure this
/// logger, see [`log_config`]. It is used like the pritln/format macros.
/// Also see [`debug`], [`info`], [`warn`], [`error`], or [`fatal`].
/// # Examples
/// ```
/// use nioruntime_log::*;
/// // log level must be set before calling any logging function.
/// // typically it is done at the top of a file so that it's easy to change.
/// // but it can be done at any level or scope. The inner scope prevails.
/// trace!(); // set log level to trace "0"
///
/// let abc = 123;
/// trace!("my value = {}", abc);
/// trace!("hi");
///
/// // The output will look like this:
/// // [2021-08-09 19:41:37]: my value = 123
/// // [2021-08-09 19:41:37]: hi
/// ```
#[macro_export]
macro_rules! trace {
        () => {
                nioruntime_log::do_log!(nioruntime_log::TRACE);
        };
        ($a:expr) => {
                {
                        nioruntime_log::log!(nioruntime_log::TRACE, $a);
                }
        };
        ($a:expr,$($b:tt)*)=>{
                {
                        nioruntime_log::log!(nioruntime_log::TRACE, $a, $($b)*);
                }
        };
}

/// Just like [`trace`], but with no timestamp.
#[macro_export]
macro_rules! trace_no_ts {
        ($a:expr) => {
                {
                        nioruntime_log::log_no_ts!(nioruntime_log::TRACE, $a);
                }
        };
        ($a:expr,$($b:tt)*)=>{
                {
                        nioruntime_log::log_no_ts!(nioruntime_log::TRACE, $a, $($b)*);
                }
        };
}

/// log_multi is identical to [`log`] except that the name of the logger is specified instead of using
/// the default logger.
/// # Examples
/// ```
/// use nioruntime_log::*;
/// // log level must be set before calling any logging function.
/// // typically it is done at the top of a file so that it's easy to change.
/// // but it can be done at any level or scope. The inner scope prevails.
/// info!();
///
/// let abc = 123;
/// log_multi!(nioruntime_log::WARN, "logger2", "hi");
/// log_multi!(nioruntime_log::WARN, "logger2", "value = {}", abc);
///
/// ```
#[macro_export]
macro_rules! log_multi {
	($level:expr, $a:expr, $b:expr) => {
		let static_log = &nioruntime_log::STATIC_LOG;
		let mut log_map = static_log.lock();
		match log_map {
			Ok(mut log_map) => {
				let log = log_map.get_mut($a);
				match log {
					Some(log) => {
						nioruntime_log::do_log!($level, true, log, $b);
					},
					None => {
						let mut log = nioruntime_log::Log::new();
						nioruntime_log::do_log!($level, true, log, $b);
						log_map.insert($a.to_string(), log);
					}
				}
			},
			Err(e) => {
				println!(
					"Error: could not log '{}' due to PoisonError: {}",
					format!($b),
					e.to_string()
				);
			}
		}
	};
	($level:expr, $a:expr,$b:expr,$($c:tt)*)=>{
		let static_log = &nioruntime_log::STATIC_LOG;
		let mut log_map = static_log.lock();
		match log_map {
			Ok(mut log_map) => {
				let log = log_map.get_mut($a);
				match log {
					Some(log) => {
						nioruntime_log::do_log!($level, true, log, $b, $($c)*);
					},
					None => {
						let mut log = nioruntime_log::Log::new();
						nioruntime_log::do_log!($level, true, log, $b, $($c)*);
						log_map.insert($a.to_string(), log);
					}
				}
			},
			Err(e) => {
				println!(
					"Error: could not log '{}' due to PoisonError: {}",
					format!($b, $($c)*),
					e.to_string()
				);
			},
		}
	};
}

/// The main logging macro. This macro calls the default logger. To configure this
/// logger, see [`log_config`]. It is used like the pritln/format macros. The first
/// parameter is the log level. To avoid specifying level, see [`trace`], [`debug`],
/// [`info`], [`warn`], [`error`], or [`fatal`].
/// # Examples
/// ```
/// use nioruntime_log::*;
///
/// info!(); // set log level to info "2"
///
/// let abc = 123;
/// log!(nioruntime_log::INFO, "my value = {}", abc);
/// log!(nioruntime_log::INFO, "hi");
///
/// // The output will look like this:
/// // [2021-08-09 19:41:37]: my value = 123
/// // [2021-08-09 19:41:37]: hi
/// ```
#[macro_export]
macro_rules! log {
	($level:expr, $a:expr)=>{
		{
                	const DEFAULT_LOG: &str = "default";
                	let static_log = &nioruntime_log::STATIC_LOG;
                	let mut log_map = static_log.lock();
			match log_map {
				Ok(mut log_map) => {
                	let log = log_map.get_mut(&DEFAULT_LOG.to_string());
                	match log {
                        	Some(log) => {
                                	nioruntime_log::do_log!($level, true, log, $a);
                        	},
                        	None => {
                                	let mut log = nioruntime_log::Log::new();
                                	nioruntime_log::do_log!($level, true, log, $a);
                                	log_map.insert(DEFAULT_LOG.to_string(), log);
                        	}
                	}
				},
				Err(e) => {
                                        println!(
                                                "Error: could not log '{}' due to PoisonError: {}",
                                                format!($a),
                                                e.to_string()
                                        );
				},
			}
		}
    	};
	($level:expr, $a:expr,$($b:tt)*)=>{
		{
                        const DEFAULT_LOG: &str = "default";
                        let static_log = &nioruntime_log::STATIC_LOG;
                        let mut log_map = static_log.lock().unwrap();
                        let log = log_map.get_mut(&DEFAULT_LOG.to_string());
                        match log {
                                Some(log) => {
                                        nioruntime_log::do_log!($level, true, log, $a, $($b)*);
                                },
                                None => {
                                        let mut log = nioruntime_log::Log::new();
                                        nioruntime_log::do_log!($level, true, log, $a, $($b)*);
                                        log_map.insert(DEFAULT_LOG.to_string(), log);
                                }
                        }
		}
	}
}

/// Identical to [`log_no_ts`] except that the name of the logger is specified instead of using
/// the default logger.
/// # Examples
///
/// ```
/// use nioruntime_log::*;
///
/// info!();
///
/// log_no_ts_multi!(2, "nondefaultlogger", "hi");
/// log_no_ts_multi!(2, "nondefaultlogger", "value = {}", 123);
/// ```
///
#[macro_export]
macro_rules! log_no_ts_multi {
        ($level:expr, $a:expr, $b:expr)=>{
                {
                        let static_log = &nioruntime_log::STATIC_LOG;
                        let mut log_map = static_log.lock().unwrap();
                        let log = log_map.get_mut($a);
                        match log {
                                Some(log) => {
                                        { nioruntime_log::do_log!($level, false, log, $b); }
                                },
                                None => {
                                        let mut log = nioruntime_log::Log::new();
                                        { nioruntime_log::do_log!($level, false, log, $b); }
                                        log_map.insert($a.to_string(), log);
                                }
                        }
                }
        };
        ($level:expr, $a:expr,$b:expr,$($c:tt)*)=>{
                {
                        let static_log = &nioruntime_log::STATIC_LOG;
                        let mut log_map = static_log.lock().unwrap();
                        let log = log_map.get_mut($a);
                        match log {
                                Some(log) => {
                                        { nioruntime_log::do_log!($level, false, log, $b, $($c)*) }
                                },
                                None => {
                                        let mut log = nioruntime_log::Log::new();
                                        { nioruntime_log::do_log!($level, false, log, $b, $($c)*) }
                                        log_map.insert($a.to_string(), log);
                                }
                        }
                }
        };
}

/// Log using the default logger and don't print a timestamp. See [`log`] for more details on logging.
/// # Examples
///
/// ```
/// use nioruntime_log::*;
///
/// debug!();
///
/// log!(2, "hi");
/// log_no_ts!(2, "message here");
/// log_no_ts!(3, "my value = {}", 1);
/// log!(2, "more data");
///
/// // The output will look like this:
/// // [2021-08-09 19:41:37]: hi
/// // message here
/// // my value = 1
/// // [2021-08-09 19:41:37]: more data
/// ```
#[macro_export]
macro_rules! log_no_ts {
	($level:expr, $a:expr)=>{
                {
                        const DEFAULT_LOG: &str = "default";
                        let static_log = &nioruntime_log::STATIC_LOG;
                        let mut log_map = static_log.lock().unwrap();
                        let log = log_map.get_mut(&DEFAULT_LOG.to_string());
                        match log {
                                Some(log) => {
                                        { nioruntime_log::do_log!($level, false, log, $a); }
                                },
                                None => {
                                        let mut log = nioruntime_log::Log::new();
                                        { nioruntime_log::do_log!($level, false, log, $a); }
                                        log_map.insert(DEFAULT_LOG.to_string(), log);
                                }
                        }
                }
	};
	($level:expr, $a:expr,$($b:tt)*)=>{
		{

                        const DEFAULT_LOG: &str = "default";
                        let static_log = &nioruntime_log::STATIC_LOG;
                        let mut log_map = static_log.lock().unwrap();
                        let log = log_map.get_mut(&DEFAULT_LOG.to_string());
                        match log {
                                Some(log) => {
                                        { nioruntime_log::do_log!($level, false, log, $a, $($b)*) }
                                },
                                None => {
                                        let mut log = nioruntime_log::Log::new();
                                        { nioruntime_log::do_log!($level, false, log, $a, $($b)*) }
                                        log_map.insert(DEFAULT_LOG.to_string(), log);
                                }
                        }
		}
	};
}

/// Generally, this macro should not be used directly. It is used by the other macros. See [`log`] or [`info`] instead.
#[macro_export]
macro_rules! do_log {
        ($level:expr)=>{
					const LOG_LEVEL: i32 = $level;
	};
        ($level:expr, $show_ts:expr, $log:expr, $a:expr)=>{
			{
                                        // if not configured, use defaults
                                        if !$log.is_configured() {
                                                $log.config_with_object(nioruntime_log::LogConfig::default()).unwrap();
                                        }

					let _ = $log.update_show_timestamp($show_ts);

						if $level >= LOG_LEVEL {
                                        		match $log.log(&format!($a)) {
                                                		Ok(_) => {},
                                                		Err(e) => {
                                                        		println!(
                                                                		"Logging of '{}' resulted in Error: {}",
                                                                		format!($a),
                                                                		e.to_string(),
                                                        		);
                                                		}
                                        		}
						}

					// always set to showing timestamp (as default)
					let _ = $log.update_show_timestamp(true);
			}
        };
        ($level:expr, $show_ts:expr, $log:expr, $a:expr, $($b:tt)*)=>{
			{
                                        // if not configured, use defaults
                                        if !$log.is_configured() {
                                                $log.config_with_object(nioruntime_log::LogConfig::default()).unwrap();
                                        }

					let _ = $log.update_show_timestamp($show_ts);

					if $level >= LOG_LEVEL {
                                        	match $log.log(&format!($a, $($b)*)) {
                                                	Ok(_) => {},
                                                	Err(e) => {
                                                        	println!(
                                                                	"Logging of '{}' resulted in Error: {}",
                                                                	format!($a, $($b)*),
                                                                	e.to_string(),
                                                        	);
                                                	}
                                        	}
					}
			}
        };
}

/// log_config_multi is identical to [`log_config`] except that the name of the logger is specified instead of using
/// the default logger.
///
/// A sample log_config_multi! call might look something like this:
///
/// ```
/// use nioruntime_log::*;
///
/// info!();
///
/// log_config_multi!(
///     "nondefaultlogger",
///     LogConfig {
///         max_age_millis: 10000, // set log rotations to every 10 seconds
///         max_size: 10000, // set log rotations to every 10,000 bytes
///         ..Default::default()
///     }
/// );
/// ```
///
/// For full details on all parameters of LogConfig see [`LogConfig`].
#[macro_export]
macro_rules! log_config_multi {
	($a:expr, $b:expr) => {{
		let static_log = &nioruntime_log::STATIC_LOG;
		let mut log_map = static_log.lock();
		match log_map {
			Ok(mut log_map) => {
				let log = log_map.get_mut($a);
				match log {
					Some(log) => log.config_with_object($b),
					None => {
						let mut log = nioruntime_log::Log::new();
						let ret = log.config_with_object($b);
						log_map.insert($a.to_string(), log);
						ret
					}
				}
			}
			Err(e) => Err(nioruntime_err::ErrorKind::PoisonError(format!(
				"log generated poison error: {}",
				e
			))
			.into()),
		}
	}};
}

/// This macro may be used to configure logging. If it is not called. The default LogConfig is used.
/// By default logging is only done to stdout.
/// A sample log_config! call might look something like this:
///
/// ```
/// use nioruntime_log::*;
///
/// info!();
///
/// log_config!(nioruntime_log::LogConfig {
/// 	max_age_millis: 10000, // set log rotations to every 10 seconds
/// 	max_size: 10000, // set log rotations to every 10,000 bytes
/// 	..Default::default()
/// });
/// ```
/// For full details on all parameters of LogConfig see [`LogConfig`].
#[macro_export]
macro_rules! log_config {
	($a:expr) => {{
		const DEFAULT_LOG: &str = "default";
		let static_log = &nioruntime_log::STATIC_LOG;
		let mut log_map = static_log.lock();
		match log_map {
			Ok(mut log_map) => {
				let log = log_map.get_mut(&DEFAULT_LOG.to_string());
				match log {
					Some(log) => log.config_with_object($a),
					None => {
						let mut log = nioruntime_log::Log::new();
						let ret = log.config_with_object($a);
						log_map.insert(DEFAULT_LOG.to_string(), log);
						ret
					}
				}
			}
			Err(e) => Err(nioruntime_err::ErrorKind::PoisonError(format!(
				"log generated poison error: {}",
				e
			))
			.into()),
		}
	}};
}

/// The main logging object
pub struct Log {
	params: Option<LogParams>,
}

/// The data that is held by the Log object
struct LogParams {
	file: Option<File>,
	cur_size: u64,
	init_age_millis: u128,
	config: LogConfig,
	has_rotated: bool,
}

pub enum RotationStatus {
	NotNeeded,
	Needed,
	AutoRotated,
}

/// Log Config object.
pub struct LogConfig {
	/// The path to the log file. By default, logging is only printed to standard output.
	/// This default behaviour is acheived by setting file_path to an empty string.
	/// If you wish to log to a file, this parameter must be set to a valid path.
	pub file_path: String,
	/// The maximum size in bytes of the log file before a log rotation occurs. By default,
	/// this is set to 10485760 bytes (10 mb). After a log rotation, a new file named:
	/// <log_name>.r_<month>_<day>_<year>_<hour>-<minute>-<second>_<random_number>.log
	/// For example, something like this: mainlog.r_08_10_2021_03-12-23_12701992901411981750.log
	/// is created.
	pub max_size: u64,
	/// The maximum age in milliseconds before a log rotation occurs. By default, this is set to
	/// 3600000 ms (1 hour). After a log rotation, a new file named:
	/// <log_name>.r_<month>_<day>_<year>_<hour>-<minute>-<second>_<random_number>.log
	/// For example, something like this: mainlog.r_08_10_2021_03-12-23_12701992901411981750.log
	/// is created.
	pub max_age_millis: u128,
	/// The header (first line) of a log file. By default the header is not printed.
	pub file_header: String,
	/// Whether or not to show the timestamp. By default, this is set to true.
	pub show_timestamp: bool,
	/// Whether or not to print the log lines to standard output. By default, this is set to true.
	pub show_stdout: bool,
	/// delete the rotated log immidiately (only used for testing)
	pub delete_rotation: bool,
}

impl Default for LogConfig {
	fn default() -> Self {
		LogConfig {
			file_path: "".to_string(),
			max_size: 1024 * 1024 * 10,     // 10 mb
			max_age_millis: 60 * 60 * 1000, // 1 hr
			file_header: "".to_string(),
			show_timestamp: true,
			show_stdout: true,
			delete_rotation: false,
		}
	}
}

impl LogParams {
	/// This function rotates logs
	pub fn rotate(&mut self) -> Result<(), Error> {
		let now: DateTime<Utc> = Utc::now();
		let rotation_string = now.format(".r_%m_%d_%Y_%T").to_string().replace(":", "-");
		let file_path = match self.config.file_path.rfind(".") {
			Some(pos) => &self.config.file_path[0..pos],
			_ => &self.config.file_path,
		};
		let file_path = format!(
			"{}{}_{}.log",
			file_path,
			rotation_string,
			rand::random::<u64>(),
		);
		if self.config.delete_rotation {
			std::fs::remove_file(&self.config.file_path)?;
		} else {
			std::fs::rename(&self.config.file_path, file_path.clone())?;
		}
		self.file = Some(
			OpenOptions::new()
				.append(true)
				.create(true)
				.open(&self.config.file_path)?,
		);

		let start_time = *START_TIME;
		let time_now = Instant::now().duration_since(start_time).as_millis();

		let mut file = self.file.as_ref().unwrap();
		let line_bytes = self.config.file_header.as_bytes();
		if line_bytes.len() > 0 {
			file.write(line_bytes)?;
			file.write(&[10u8])?; // new line
		}
		self.init_age_millis = time_now;
		self.cur_size = self.config.file_header.len() as u64 + 1;

		Ok(())
	}

	pub fn rotation_status(&mut self) -> Result<RotationStatus, Error> {
		// get current time
		let start_time = *START_TIME;
		let time_now = Instant::now().duration_since(start_time).as_millis();
		if self.file.is_some()
			&& (self.cur_size >= self.config.max_size
				|| time_now.saturating_sub(self.init_age_millis) > self.config.max_age_millis)
		{
			Ok(RotationStatus::Needed)
		} else if self.has_rotated {
			self.has_rotated = false;
			Ok(RotationStatus::AutoRotated)
		} else {
			Ok(RotationStatus::NotNeeded)
		}
	}

	/// The actual logging function, handles rotation if needed
	pub fn log(&mut self, line: &str) -> Result<(), Error> {
		let line_bytes = line.as_bytes(); // get line as bytes
		self.cur_size += line_bytes.len() as u64 + 1; // increment cur_size
		if self.config.show_timestamp {
			// timestamp is an additional 23 bytes
			self.cur_size += 23;
		}
		// get current time
		let start_time = *START_TIME;
		let time_now = Instant::now().duration_since(start_time).as_millis();

		// check if rotation is needed
		if self.file.is_some()
			&& (self.cur_size >= self.config.max_size
				|| time_now.saturating_sub(self.init_age_millis) > self.config.max_age_millis)
		{
			self.has_rotated = true;
			self.rotate()?;
		}

		// if we're showing the timestamp, print it
		if self.config.show_timestamp {
			let date = Local::now();
			let formatted_ts = date.format("%Y-%m-%d %H:%M:%S");
			if self.file.is_some() {
				self.file
					.as_ref()
					.unwrap()
					.write(format!("[{}]: ", formatted_ts).as_bytes())?;
			}
			if self.config.show_stdout {
				print!("[{}]: ", formatted_ts);
			}
		}
		// finally log the line followed by a newline.
		if self.file.is_some() {
			let mut file = self.file.as_ref().unwrap();
			file.write(line_bytes)?;
			file.write(&[10u8])?; // newline
		}

		// if stdout is specified log to stdout too
		if self.config.show_stdout {
			println!("{}", line);
		}

		Ok(())
	}
}

impl Log {
	/// create a new Log object
	pub fn new() -> Log {
		Log { params: None }
	}

	pub fn is_configured(&self) -> bool {
		self.params.is_some()
	}

	/// configure with object
	pub fn config_with_object(&mut self, config: LogConfig) -> Result<(), Error> {
		self.config(
			match config.file_path.len() == 0 {
				true => None,
				false => Some(config.file_path),
			},
			config.max_size,
			config.max_age_millis,
			config.show_timestamp,
			&config.file_header,
			config.show_stdout,
			config.delete_rotation,
		)?;
		Ok(())
	}

	/// configure the log
	pub fn config(
		&mut self,
		file_path: Option<String>,
		max_size: u64,
		max_age_millis: u128,
		show_timestamp: bool,
		file_header: &str,
		show_stdout: bool,
		delete_rotation: bool,
	) -> Result<(), Error> {
		// create file with append option and create option
		let file = match file_path.clone() {
			Some(file_path) => Some(
				OpenOptions::new()
					.append(true)
					.create(true)
					.open(file_path)?,
			),
			None => None,
		};

		// get current size of the file
		let mut cur_size = match file_path.clone() {
			Some(file_path) => metadata(file_path)?.len(),
			None => 0,
		};

		// age is only relative to start logging time
		let start_time = *START_TIME;
		let init_age_millis = Instant::now().duration_since(start_time).as_millis();

		let file_path = match file_path {
			Some(file_path) => Some(
				canonicalize(PathBuf::from(file_path))?
					.into_os_string()
					.into_string()?,
			),
			None => None,
		};

		let file_header = file_header.to_string();
		if cur_size == 0 && file_path.is_some() {
			// add the header if the file is new
			let line_bytes = file_header.as_bytes();
			if line_bytes.len() > 0 {
				let mut file = file.as_ref().unwrap();
				file.write(line_bytes)?;
				file.write(&[10u8])?; // new line
				cur_size = file_header.len() as u64 + 1;
			}
		}

		self.params = Some(LogParams {
			file,
			cur_size,
			init_age_millis,
			config: LogConfig {
				max_size,
				file_path: file_path.unwrap_or("".to_string()),
				max_age_millis,
				show_timestamp,
				file_header,
				show_stdout,
				delete_rotation,
			},
			has_rotated: false,
		});

		Ok(())
	}

	// rotate the log
	pub fn rotate(&mut self) -> Result<(), Error> {
		match self.params.as_mut() {
			Some(params) => params.rotate(),
			None => Err(ErrorKind::LogNotConfigured("log params None".to_string()).into()),
		}
	}

	// check if a rotation is needed
	pub fn rotation_status(&mut self) -> Result<RotationStatus, Error> {
		match self.params.as_mut() {
			Some(params) => params.rotation_status(),
			None => Err(ErrorKind::LogNotConfigured("log params None".to_string()).into()),
		}
	}

	/// Entry point for logging
	pub fn log(&mut self, line: &str) -> Result<(), Error> {
		match self.params.as_mut() {
			Some(params) => {
				params.log(line)?;
				Ok(())
			}
			None => Err(ErrorKind::LogNotConfigured("log params None".to_string()).into()),
		}
	}

	/// Update the show_timestamp parameter for this log
	pub fn update_show_timestamp(&mut self, show: bool) -> Result<(), Error> {
		match self.params.as_mut() {
			Some(params) => {
				params.config.show_timestamp = show;
				Ok(())
			}
			None => Err(ErrorKind::LogNotConfigured("log params None".to_string()).into()),
		}
	}

	/// Update the show_stdout parameter for this log
	pub fn update_show_stdout(&mut self, show: bool) -> Result<(), Error> {
		match self.params.as_mut() {
			Some(params) => {
				params.config.show_stdout = show;
				Ok(())
			}
			None => Err(ErrorKind::LogNotConfigured("log params None".to_string()).into()),
		}
	}
}
