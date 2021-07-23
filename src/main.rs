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

use nioruntime_kqueues::kqueues::KqueueEventHandler;
use nioruntime_libnio::EventHandler;
use nioruntime_util::Error;
use std::net::TcpListener;

fn main() {
	let res = real_main();
	match res {
		Ok(_) => {}
		Err(e) => println!("real_main generated Error: {}", e.to_string()),
	}
}

fn real_main() -> Result<(), Error> {
	let listener = TcpListener::bind("127.0.0.1:9999")?;
	let mut kqe = KqueueEventHandler::new();

	kqe.set_on_read(move |id, buf, len| {
		let utf8_ver = std::str::from_utf8(&buf[0..len as usize]);
		match utf8_ver {
			Ok(s) => {
				println!("closure read[{}]={}", id, s);
			}
			Err(e) => {
				println!("error: {}", e.to_string());
			}
		}
	})?;

	kqe.add_tcp_listener(&listener)?;

	let mut i = 0;
	loop {
		std::thread::sleep(std::time::Duration::from_secs(10));
		//let _ = kqe.wakeup();
		kqe.write(7, b"hi\n")?;
		i += 1;
		if i == 10000000 {
			break;
		}
	}
	std::thread::park();
	Ok(())
}
