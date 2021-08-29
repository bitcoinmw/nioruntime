# NIO Runtime

NIO Runtime is a non-blocking I/O runtime for Rust. It supports epoll (Linux), wepoll (Windows), and Kqueues (BSD variants including macosx) for highly performant client/server applications.

# Performance

NIO Runtime is designed to be high throughput / low latency. Benchmarks show that NIO Runtime is approximately as performant as [nginx](http://nginx.com). In certain circumstances (as in the Linux server that was used for testing), the throughput is slightly better than Nginx. Nginx's latency is slightly faster, although both are very good on both metrics. Having a comparable performing server written in Rust will likely be useful for people looking to write performant server side applications in Rust. Here are a few runs of the benchmark tool which is included in the [Rustlet](https://github.com/bitcoinmw/rustlet) project.

Rustlet with 100 request per connection, 100 threads, and 10 iterations.
```
$ ./target/release/rustlet -c -x 100 -t 100 -i 10
[2021-08-28 20:21:51]: Running client 
[2021-08-28 20:21:51]: Threads=100
[2021-08-28 20:21:51]: Iterations=10
[2021-08-28 20:21:51]: Requests per thread per iteration=100
--------------------------------------------------------------------------------
[2021-08-28 20:21:51]: Iteration 1 complete. 
[2021-08-28 20:21:52]: Iteration 2 complete. 
[2021-08-28 20:21:52]: Iteration 3 complete. 
[2021-08-28 20:21:52]: Iteration 4 complete. 
[2021-08-28 20:21:52]: Iteration 5 complete. 
[2021-08-28 20:21:52]: Iteration 6 complete. 
[2021-08-28 20:21:52]: Iteration 7 complete. 
[2021-08-28 20:21:52]: Iteration 8 complete. 
[2021-08-28 20:21:52]: Iteration 9 complete. 
[2021-08-28 20:21:52]: Iteration 10 complete. 
--------------------------------------------------------------------------------
[2021-08-28 20:21:52]: Test complete in 914 ms
[2021-08-28 20:21:52]: QPS=109409.19037199125
[2021-08-28 20:21:52]: Average latency=0.50975372872ms
[2021-08-28 20:21:52]: Max latency=59.528915ms
```

Nginx with 100 request per connection, 100 threads, and 10 iterations.
```
$ ./target/release/rustlet -c -x 100 -t 100 -i 10 --nginx
[2021-08-28 20:21:48]: Running client against nginx
[2021-08-28 20:21:48]: Threads=100
[2021-08-28 20:21:48]: Iterations=10
[2021-08-28 20:21:48]: Requests per thread per iteration=100
--------------------------------------------------------------------------------
[2021-08-28 20:21:48]: Iteration 1 complete. 
[2021-08-28 20:21:48]: Iteration 2 complete. 
[2021-08-28 20:21:49]: Iteration 3 complete. 
[2021-08-28 20:21:49]: Iteration 4 complete. 
[2021-08-28 20:21:49]: Iteration 5 complete. 
[2021-08-28 20:21:49]: Iteration 6 complete. 
[2021-08-28 20:21:49]: Iteration 7 complete. 
[2021-08-28 20:21:49]: Iteration 8 complete. 
[2021-08-28 20:21:49]: Iteration 9 complete. 
[2021-08-28 20:21:49]: Iteration 10 complete. 
--------------------------------------------------------------------------------
[2021-08-28 20:21:49]: Test complete in 935 ms
[2021-08-28 20:21:49]: QPS=106951.871657754
[2021-08-28 20:21:49]: Average latency=0.3121146425ms
[2021-08-28 20:21:49]: Max latency=19.335026ms
```

Rustlet with 1,000 request per connection, 100 threads, and 10 iterations.

```
$ ./target/release/rustlet -c -x 1000 -t 100 -i 10
[2021-08-28 20:27:08]: Running client 
[2021-08-28 20:27:08]: Threads=100
[2021-08-28 20:27:08]: Iterations=10
[2021-08-28 20:27:08]: Requests per thread per iteration=1000
--------------------------------------------------------------------------------
[2021-08-28 20:27:08]: Iteration 1 complete. 
[2021-08-28 20:27:09]: Iteration 2 complete. 
[2021-08-28 20:27:10]: Iteration 3 complete. 
[2021-08-28 20:27:10]: Iteration 4 complete. 
[2021-08-28 20:27:11]: Iteration 5 complete. 
[2021-08-28 20:27:12]: Iteration 6 complete. 
[2021-08-28 20:27:12]: Iteration 7 complete. 
[2021-08-28 20:27:13]: Iteration 8 complete. 
[2021-08-28 20:27:14]: Iteration 9 complete. 
[2021-08-28 20:27:14]: Iteration 10 complete. 
--------------------------------------------------------------------------------
[2021-08-28 20:27:14]: Test complete in 6821 ms
[2021-08-28 20:27:14]: QPS=146606.06949127692
[2021-08-28 20:27:14]: Average latency=0.595558146914ms
[2021-08-28 20:27:14]: Max latency=48.482466ms
```

As seen by the numbers above, throughput is slightly better under these circumstances on this particular piece of hardware (6 core intel CPU i5-9400 CPU @ 2.90GHz). Latency for both is in the sub millisecond range.

NIO Runtime also has it's own benchmark included in the project itself. Here's the output of one such run of the benchmark:

```
$ ../nioruntime/target/release/nioruntime -c -x 1000 -t 100 -i 10 
[2021-08-28 20:33:12]: Running client
[2021-08-28 20:33:12]: Threads=100
[2021-08-28 20:33:12]: Iterations=10
[2021-08-28 20:33:12]: Requests per thread per iteration=1000
[2021-08-28 20:33:12]: Request length: Max=100,Min=1
--------------------------------------------------------------------------------
[2021-08-28 20:33:12]: Iteration 1 complete. 
[2021-08-28 20:33:13]: Iteration 2 complete. 
[2021-08-28 20:33:13]: Iteration 3 complete. 
[2021-08-28 20:33:14]: Iteration 4 complete. 
[2021-08-28 20:33:14]: Iteration 5 complete. 
[2021-08-28 20:33:15]: Iteration 6 complete. 
[2021-08-28 20:33:15]: Iteration 7 complete. 
[2021-08-28 20:33:15]: Iteration 8 complete. 
[2021-08-28 20:33:16]: Iteration 9 complete. 
[2021-08-28 20:33:16]: Iteration 10 complete. 
--------------------------------------------------------------------------------
[2021-08-28 20:33:16]: Test complete in 4245 ms
[2021-08-28 20:33:16]: QPS=235571.26030624265
[2021-08-28 20:33:16]: Average latency=0.304248039477ms
[2021-08-28 20:33:16]: Max latency=145.604269ms
```

Without the overhead of HTTP and the Rustlet container, the performance of NIO Runtime is even better as seen by the numbers above.
