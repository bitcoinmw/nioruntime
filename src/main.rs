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
	kqe.set_on_read(move |_connection_id, _message_id, buf, len| (buf, 0, len))?;
	kqe.set_on_accept(move |connection_id| println!("accept conn: {}", connection_id))?;
	kqe.set_on_close(move |connection_id| println!("close conn: {}", connection_id))?;
	kqe.set_on_write_success(move |connection_id, message_id| {
		println!(
			"message success for cid={},mid={}",
			connection_id, message_id
		);
	})?;
	kqe.set_on_write_fail(move |connection_id, message_id| {
		println!("message fail for cid={},mid={}", connection_id, message_id);
	})?;
	kqe.add_tcp_listener(&listener)?;
	std::thread::park();
	Ok(())
}
