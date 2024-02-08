use std::net::{ TcpListener, TcpStream };
use std::io::{ Result, Read, Write };

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                handle_client_connection(&mut stream).unwrap();
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client_connection(stream: &mut TcpStream) -> Result<()> {
    let mut buffer: [u8; 1024] = [0; 1024];
    let _bytes_read = stream.read(&mut buffer)?;
    let foo = String::from_utf8_lossy(&buffer);
    dbg!(foo);
    for _command in buffer.split(|&byte| byte == b'\n') {
        stream.write_all(b"+PONG\r\n")?
    }
    Ok(())
}
