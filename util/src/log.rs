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

use crate::{Error, ErrorKind};
use chrono::{DateTime, Local, Utc};
use lazy_static::lazy_static;
use std::fs::{canonicalize, metadata, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;

lazy_static! {
	pub static ref LOG: Arc<Mutex<Log>> = Arc::new(Mutex::new(Log::new()));
}

/*
  Example:
  log_config!(LogConfig { .. });
  log!("ok ok ok");
  log!("test {}", 1);
*/

#[macro_export]
macro_rules! log {
	($a:expr)=>{
		{
                        use nioruntime_util::LogConfig;
                        let log = &nioruntime_util::LOG;
                        let log = log.lock();
                        match log {
                                Ok(mut log) => {
                                        // if not configured, use defaults
                                        if !log.is_configured() {
                                                log.config_with_object(LogConfig::default()).unwrap();
                                        }

                                        match log.log(&format!($a)) {
                                                Ok(_) => {},
                                                Err(e) => {
                                                        println!(
                                                                "Logging of '{}' resulted in Error: {}",
                                                                format!($a),
                                                                e.to_string(),
                                                        );
                                                }
                                        }

                                },
                                Err(e) => {
                                        println!(
                                                "Error: could not use logger to log '{}' due to PoisonError: {}",
                                                format!($a),
                                                e.to_string()
                                        );
                                },
                        }
		}
    	};
	($a:expr,$($b:tt)*)=>{
		{
			use nioruntime_util::LogConfig;
			let log = &nioruntime_util::LOG;
			let log = log.lock();
			match log {
				Ok(mut log) => {
					// if not configured, use defaults
					if !log.is_configured() {
						log.config_with_object(LogConfig::default()).unwrap();
					}

					match log.log(&format!($a, $($b)*)) {
						Ok(_) => {},
						Err(e) => {
							println!(
								"Logging of '{}' resulted in Error: {}",
								format!($a, $($b)*),
								e.to_string(),
							);
						}
					}

				},
				Err(e) => {
					println!(
						"Error: could not use logger to log '{}' due to PoisonError: {}",
						format!($a, $($b)*),
						e.to_string()
					);
				},
			}
		}

	}
}

#[macro_export]
macro_rules! log_config {
	($a:expr) => {{
		use nioruntime_util::LogConfig;
		let log = &nioruntime_util::LOG;
		let log = log.lock();

		match log {
			Ok(mut log) => log.config_with_object($a),
			Err(e) => {
				Err(ErrorKind::PoisonError(format!("log generated poison error: {}", e)).into())
			}
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
}

pub struct LogConfig {
	file_path: String,
	max_size: u64,
	max_age_millis: u128,
	file_header: String,
	show_timestamp: bool,
	show_stdout: bool,
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
		}
	}
}

impl LogParams {
	/// This function rotates logs
	fn rotate(&mut self) -> Result<(), Error> {
		let now: DateTime<Utc> = Utc::now();
		let rotation_string = now.format(".r_%m_%e_%Y_%T").to_string().replace(":", "-");
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
		std::fs::rename(&self.config.file_path, file_path.clone())?;
		self.file = Some(
			OpenOptions::new()
				.append(true)
				.create(true)
				.open(&self.config.file_path)?,
		);
		Ok(())
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
		let time_now = SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("Time went backwards")
			.as_millis();

		// check if rotation is needed
		if self.file.is_some()
			&& (self.cur_size >= self.config.max_size
				|| time_now.saturating_sub(self.init_age_millis) > self.config.max_age_millis)
		{
			self.rotate()?;
			let mut file = self.file.as_ref().unwrap();
			let line_bytes = self.config.file_header.as_bytes();
			file.write(line_bytes)?;
			file.write(&[10u8])?; // new line
			self.init_age_millis = time_now;
			self.cur_size = self.config.file_header.len() as u64 + 1;
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
		)?;
		Ok(())
	}

	/// configure the logger
	pub fn config(
		&mut self,
		file_path: Option<String>,
		max_size: u64,
		max_age_millis: u128,
		show_timestamp: bool,
		file_header: &str,
		show_stdout: bool,
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
		let init_age_millis = SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("Time went backwards")
			.as_millis();
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
			let mut file = file.as_ref().unwrap();
			file.write(line_bytes)?;
			file.write(&[10u8])?; // new line
			cur_size = file_header.len() as u64 + 1;
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
			},
		});

		Ok(())
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

	/// Update the show_timestamp parameter for this logger
	pub fn update_show_timestamp(&mut self, show: bool) -> Result<(), Error> {
		match self.params.as_mut() {
			Some(params) => {
				params.config.show_timestamp = show;
				Ok(())
			}
			None => Err(ErrorKind::LogNotConfigured("log params None".to_string()).into()),
		}
	}

	/// Update the show_stdout parameter for this logger
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
