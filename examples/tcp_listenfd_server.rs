// You can run this example from the root of the mio repo:
// cargo run --example tcp_listenfd_server --features="os-poll net"
// or with wasi:
// cargo +nightly build --target wasm32-wasip1  --example tcp_listenfd_server --features="os-poll net"
// wasmtime run --tcplisten 127.0.0.1:9000 --env 'LISTEN_FDS=1' target/wasm32-wasip1/debug/examples/tcp_listenfd_server.wasm
// 使用telnet 进行客户端链接测试
// telnet 127.0.0.1 9000
//
// ```bash
// telnet 127.0.0.1 9000
// Trying 127.0.0.1...
// Connected to 127.0.0.1.
// Escape character is '^]'.
// Hello world!
// ```
// 客户端输入数据会在server端持续接收并输出
// Ctrl + ] 输入quit断开链接

use mio::event::Event;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Registry, Token};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::str::from_utf8;

// Setup some tokens to allow us to identify which event is for which socket.
const SERVER: Token = Token(0);

// Some data we'll send over the connection.
const DATA: &[u8] = b"Hello world!\n";

#[cfg(not(windows))]
fn get_first_listen_fd_listener() -> Option<std::net::TcpListener> {
    #[cfg(any(unix, target_os = "hermit", target_os = "wasi"))]
    use std::os::fd::FromRawFd;

    let stdlistener = unsafe { std::net::TcpListener::from_raw_fd(3) };
    stdlistener.set_nonblocking(true).unwrap();
    Some(stdlistener)
}

#[cfg(windows)]
fn get_first_listen_fd_listener() -> Option<std::net::TcpListener> {
    // Windows does not support `LISTEN_FDS`
    None
}

fn main() -> io::Result<()> {
    env_logger::init();

    // std::env::var("LISTEN_FDS").expect("LISTEN_FDS environment variable unset");

    // 1. 这里同我们的tinymio, 以linux为例， 最终会通过epoll_create创建内核事件队列实例，并返回指向他的fd
    // Create a poll instance.
    let mut poll = Poll::new()?;
    // Create storage for events.
    let mut events = Events::with_capacity(128);

    // Setup the TCP server socket.
    // let mut server = {
    //     let stdlistener = get_first_listen_fd_listener().unwrap();
    //     println!("Using preopened socket FD 3");
    //     println!("You can connect to the server using `nc`:");
    //     match stdlistener.local_addr() {
    //         Ok(a) => println!(" $ nc {} {}", a.ip(), a.port()),
    //         Err(_) => println!(" $ nc <IP> <PORT>"),
    //     }
    //     println!("You'll see our welcome message and anything you type will be printed here.");
    //     TcpListener::from_std(stdlistener)
    // };
    //
    // Setup the TCP server socket.
    let addr = "127.0.0.1:9000".parse().unwrap();
    let mut server = TcpListener::bind(addr)?;

    // 2. 同tinymio, 注册一个事件到上面我们得到的fd对应的内核事件队列中
    // 这里注册的是一个socket 可读的事件
    // server 对应的source, SERVER是token, Interest::READABLE 是可读事件
    // server有很多，这里是一个TCPListener:
    // pub struct TcpListener {
    //     inner: IoSource<net::TcpListener>,
    // }
    // mio给他实现了event::source, 实际上是对内部inner的source封装
    // IoSource实现了这个source trait
    // impl<T> event::Source for IoSource<T>
    // {
    //     fn register(
    //         &mut self,
    //         registry: &Registry,
    //         token: Token,
    //         interests: Interest,
    //     ) -> io::Result<()> {
    //         self.state
    //             .register(registry, token, interests, self.inner.as_raw_fd())
    //     }
    // 这个T就是 我们这里的TCPListener
    // pub struct IoSource<T> {
    //     state: IoSourceState,
    //     inner: T,
    //     #[cfg(debug_assertions)]
    //     selector_id: SelectorId,
    // }
    // 这里同tinymio, 注册事件，registry 透传可以拿到epoll_fd, interests 是事件， token是标识， inner.as_raw_fd就是 这个连接的socket fd
    // Register the server with poll we can receive events for it.
    poll.registry()
        .register(&mut server, SERVER, Interest::READABLE)?;

    // 这里让这个source和这个token标识做个映射
    // 我们在event的第二个字段放了这个token进去，所以事件响应时，我们从event中可以获取到这个token
    // 然后通过映射找到这个TcpStream 读取数据进行后续处理
    // Map of `Token` -> `TcpStream`.
    // let mut connections = HashMap::new();
    let mut connections: HashMap<Token, Connection> = HashMap::new();
    // Unique token for each incoming connection.
    let mut unique_token = Token(SERVER.0 + 1);

    // 然后开始持续监听事件的响应
    // 这里poll 就是调用selector的select, 处理逻辑同我们的tinymio
    //         events.clear();
    //         syscall!(epoll_wait(
    //             self.ep.as_raw_fd(),
    //             events.as_mut_ptr(),
    //             events.capacity() as i32,
    //             timeout,
    //         ))
    //         .map(|n_events| {
    //             // This is safe because `epoll_wait` ensures that `n_events` are
    //             // assigned.
    //             unsafe { events.set_len(n_events as usize) };
    //         })
    loop {
        // 程序执行到这里会阻塞。它将 CPU 控制权交还给操作系统，几乎不消耗任何资源。
        //
        // 它会一直等待，直到它所监听的任一事件源（目前只有一个：SERVER）上的事件（目前只关心 READABLE）发生。
        //
        // 一旦有事件发生，内核会唤醒这个线程，poll 调用返回，并将所有就绪的事件填充到 events 集合中。
        poll.poll(&mut events, None)?;

        for event in events.iter() {
            match event.token() {
                // 如果发现监听到触发的事件对应的token是我们刚刚放入的SERVER token
                // 唤醒进行后续处理
                // 处理新连接
                SERVER => loop {
                    // 开始建立链接
                    // Received an event for the TCP server socket, which
                    // indicates we can accept an connection.
                    // TODO: ? 在边沿触发模式下，当有新连接到达时，内核只会通知你一次。如果在这次通知和你调用 accept() 之间，又有多个连接同时到达，内核不会再次通知你。
                    //
                    // 因此，正确的模式是：收到一次通知后，你必须在一个循环里持续调用 accept()，直到它返回错误 ErrorKind::WouldBlock。
                    // 这个错误告诉你：“所有排队的连接都已经被你 accept 完了，队列空了，可以停了。” 如果你每次只 accept 一次，就可能会丢失连接，导致客户端长时间等待。
                    let (mut connection, address) = match server.accept() {
                        Ok((connection, address)) => (connection, address),
                        Err(ref e) if would_block(e) => {
                            // If we get a `WouldBlock` error we know our
                            // listener has no more incoming connections queued,
                            // so we can return to polling and wait for some
                            // more.
                            break;
                        }
                        Err(e) => {
                            // If it was any other kind of error, something went
                            // wrong and we terminate with an error.
                            return Err(e);
                        }
                    };

                    println!("Accepted connection from: {address}");

                    // 构建新的token标识
                    let token = next(&mut unique_token);
                    // 注册新的event, 针对这个连接的可写事件
                    // 为什么注册？因为这个服务器的协议是，一旦建立连接，马上就要主动发送 "Hello world!" 这条欢迎消息。
                    // 所以它关心的是这个新 connection 何时可以写入数据。
                    poll.registry()
                        .register(&mut connection, token, Interest::WRITABLE)?;

                    // 将token和建立连接的tcpstream 的socket fd做个映射
                    // 初始时发送hello world, 之后客户端发送什么拼接一个server前缀再返回
                    let mut new_connection = Connection {
                        inner: connection,
                        send_queue: DATA.to_vec(),
                    };
                    connections.insert(token, new_connection);
                },
                // 这里响应的事件如果不是SERVER, 而是其他token,则不是tcplistene处理建立链接的逻辑，而是建立连接后tcpstream的通信逻辑
                // 处理现有连接
                token => {
                    // Maybe received an event for a TCP connection.
                    let done = if let Some(connection) = connections.get_mut(&token) {
                        // 进行后续通信流程处理
                        // 包括持续的可读可写事件注册监听，以及对应的写数据和读数据
                        // handle_connection_event(poll.registry(), connection, event)?
                        handle_connection_event_v2(poll.registry(), connection, event)?
                    } else {
                        // Sporadic events happen, we can safely ignore them.
                        false
                    };
                    // 如果链接通信完成，链接关闭了，这里要释放资源将event清除
                    // WARN: 非常重要！ 告诉 Poll 实例：“我不再关心这个连接的任何事件了，请从你的监听列表中将它移除。”
                    // 这会释放内核中与此 FD 相关的资源。如果不做这一步，会导致资源泄露。
                    if done {
                        if let Some(mut connection) = connections.remove(&token) {
                            // 清除资源
                            poll.registry().deregister(&mut connection.inner)?;
                        }
                    }
                }
            }
        }
    }
}

fn next(current: &mut Token) -> Token {
    let next = current.0;
    current.0 += 1;
    Token(next)
}

struct Connection {
    inner: TcpStream,
    // 上层缓冲区，存放待发送给客户端的数据
    send_queue: Vec<u8>,
}

/// Returns `true` if the connection is done.
fn handle_connection_event(
    registry: &Registry,
    connection: &mut TcpStream,
    event: &Event,
) -> io::Result<bool> {
    // 判断这个source 即已经建立的connection 的tcpstream的这个event类型
    // 如果是可写事件就写数据
    if event.is_writable() {
        // We can (maybe) write to the connection.
        match connection.write(DATA) {
            // We want to write the entire `DATA` buffer in a single go. If we
            // write less we'll return a short write error (same as
            // `io::Write::write_all` does).
            Ok(n) if n < DATA.len() => return Err(io::ErrorKind::WriteZero.into()),
            Ok(_) => {
                // 写完之后我们再注册一个这个链接的 socket fd的可读事件, 等到响应可读时再来读响应数据
                // After we've written something we'll reregister the connection
                // to only respond to readable events.
                registry.reregister(connection, event.token(), Interest::READABLE)?
            }
            // Would block "errors" are the OS's way of saying that the
            // connection is not actually ready to perform this I/O operation.
            Err(ref err) if would_block(err) => {}
            // Got interrupted (how rude!), we'll try again.
            Err(ref err) if interrupted(err) => {
                return handle_connection_event(registry, connection, event)
            }
            // Other errors we'll consider fatal.
            Err(err) => return Err(err),
        }
    }

    // 等待可读 -> 可读事件触发: if event.is_readable() 成立。
    //
    // 服务器在一个 loop 中反复 read 数据，直到 WouldBlock（原因同 accept 循环）。
    //
    // 如果 read 返回 Ok(0)，表示客户端主动关闭了连接。
    // 如果是可读事件
    if event.is_readable() {
        // 连接是否存活
        let mut connection_closed = false;
        // 构建读取数据的缓冲区
        let mut received_data = vec![0; 4096];
        // 读取数据的大小
        let mut bytes_read = 0;
        // We can (maybe) read from the connection.
        loop {
            // 开始读取
            match connection.read(&mut received_data[bytes_read..]) {
                Ok(0) => {
                    // Reading 0 bytes means the other side has closed the
                    // connection or is done writing, then so are we.
                    connection_closed = true;
                    break;
                }
                Ok(n) => {
                    bytes_read += n;
                    if bytes_read == received_data.len() {
                        received_data.resize(received_data.len() + 1024, 0);
                    }
                }
                // Would block "errors" are the OS's way of saying that the
                // connection is not actually ready to perform this I/O operation.
                Err(ref err) if would_block(err) => break,
                Err(ref err) if interrupted(err) => continue,
                // Other errors we'll consider fatal.
                Err(err) => return Err(err),
            }
        }

        if bytes_read != 0 {
            let received_data = &received_data[..bytes_read];
            if let Ok(str_buf) = from_utf8(received_data) {
                println!("Received data: {}", str_buf.trim_end());
            } else {
                println!("Received (none UTF-8) data: {received_data:?}");
            }
        }

        // 如果读取正常，读取之后关闭链接
        if connection_closed {
            println!("Connection closed");
            return Ok(true);
        }
    }

    Ok(false)
}

fn would_block(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::WouldBlock
}

fn interrupted(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::Interrupted
}

fn handle_connection_event_v2(
    registry: &Registry,
    connection: &mut Connection,
    event: &Event,
) -> io::Result<bool> {
    // 判断这个source 即已经建立的connection 的tcpstream的这个event类型
    // 如果是可写事件就写数据
    if event.is_writable() {
        loop {
            if connection.send_queue.is_empty() {
                // 没有东西可写了
                break;
            }
            // We can (maybe) write to the connection.
            let head = b"server response: ";
            match connection.inner.write(&connection.send_queue) {
                // We want to write the entire `DATA` buffer in a single go. If we
                // write less we'll return a short write error (same as
                // `io::Write::write_all` does).
                Ok(0) => {
                    return Err(io::ErrorKind::WriteZero.into());
                }
                Ok(n) => {
                    // 将发送的部分移除
                    connection.send_queue.drain(..n);
                }

                // Ok(_) => {
                //     // 写完之后我们再注册一个这个链接的 socket fd的可读事件, 等到响应可读时再来读响应数据
                //     // After we've written something we'll reregister the connection
                //     // to only respond to readable events.
                //     registry.reregister(&mut connection.inner, event.token(), Interest::READABLE)?
                // }

                // Would block "errors" are the OS's way of saying that the
                // connection is not actually ready to perform this I/O operation.
                Err(ref err) if would_block(err) => {}
                // Got interrupted (how rude!), we'll try again.
                Err(ref err) if interrupted(err) => {
                    return handle_connection_event_v2(registry, connection, event)
                }
                // Other errors we'll consider fatal.
                Err(err) => return Err(err),
            }
        }

        // 如果不为空，并且发送完毕之后为空说明发送成功
        // 注册可读服务，读客户端发送的消息
        if connection.send_queue.is_empty() {
            println!("响应已经全部写入，等待客户端新消息");
            registry.reregister(&mut connection.inner, event.token(), Interest::READABLE)?;
        }
    }

    // 等待可读 -> 可读事件触发: if event.is_readable() 成立。
    //
    // 服务器在一个 loop 中反复 read 数据，直到 WouldBlock（原因同 accept 循环）。
    //
    // 如果 read 返回 Ok(0)，表示客户端主动关闭了连接。
    // 如果是可读事件
    if event.is_readable() {
        // 连接是否存活
        let mut connection_closed = false;
        // 构建读取数据的缓冲区
        // let mut received_data = vec![0; 4096];
        // 改用extend之后不直接读取到该缓冲区了，所以初始化的方式指定容量就行了
        let mut received_data = Vec::with_capacity(4096);
        // 读取的中级缓冲区
        let mut temp_buf = [0; 256];

        // 读取数据的大小
        let mut bytes_read = 0;
        // We can (maybe) read from the connection.
        loop {
            // 开始读取
            match connection.inner.read(&mut temp_buf[bytes_read..]) {
                Ok(0) => {
                    // Reading 0 bytes means the other side has closed the
                    // connection or is done writing, then so are we.
                    connection_closed = true;
                    break;
                }
                Ok(n) => {
                    // 读取客户端的数据，然后进行对应逻辑处理，这里我们统一添加前缀，然后注册可写事件
                    // 可写是将数据写入socket 发送给客户端
                    bytes_read += n;
                    // 超过了，扩容
                    // if bytes_read == received_data.len() {
                    //     received_data.resize(received_data.len() + 1024, 0);
                    // }
                    // extend 是在最后面开始追加，会自动扩容
                    received_data.extend_from_slice(&temp_buf[..n]);

                    // connection.send_queue.extend_from_slice(received_data.as_slice());
                }
                // Would block "errors" are the OS's way of saying that the
                // connection is not actually ready to perform this I/O operation.
                // 没有更多可读数据了
                Err(ref err) if would_block(err) => break,
                // 重试
                Err(ref err) if interrupted(err) => continue,
                // Other errors we'll consider fatal.
                // 真正的错误
                Err(err) => return Err(err),
            }
        }

        // 打印一下读取的数据
        if bytes_read != 0 {
            let received_data = &received_data[..bytes_read];
            if let Ok(str_buf) = from_utf8(received_data) {
                println!("Received data: {}", str_buf.trim_end());
            } else {
                println!("Received (none UTF-8) data: {received_data:?}");
            }
        }

        if !received_data.is_empty() {
            // 1. 获取当前连接的token
            let token = event.token();

            // 2. 业务处理，这里添加一个对应链接的前缀
            let prefix = format!("[Response from connection {}]: ", token.0);
            println!(
                "Received {} bytes from conn {}, preparing echi.",
                received_data.len(),
                token.0
            );

            // 3. 将处理后的响应放入上层发送缓冲区，等待写入socket
            connection.send_queue.extend_from_slice(prefix.as_bytes());
            connection
                .send_queue
                .extend_from_slice(received_data.as_slice());
        }

        // 如果我们受到了新数据，证明发送队列不为空
        // 或者客户端断开连接了，需要我们处理
        if !connection.send_queue.is_empty() {
            // 我们收到请求后已经读取并且处理了，现在准备注册写事件，将处理后的数据返回给客户端
            println!("客户端的消息已经读取，并处理完毕，等待发送响应给客户端");
            registry.reregister(&mut connection.inner, event.token(), Interest::WRITABLE)?;
        }

        // 如果读取正常，读取之后关闭链接
        if connection_closed {
            println!("Connection closed");
            return Ok(true);
        }
    }

    Ok(false)
}
