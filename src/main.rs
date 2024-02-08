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
    let mut buffer: Vec<u8> = vec![0; 1024];
    let bytes_read = stream.read(&mut buffer)?;
    buffer.truncate(bytes_read);
    dbg!(String::from_utf8_lossy(&buffer));
    for command in buffer.split(|&byte| byte == b'\n') {
        dbg!(&command);
        let mut command = String::from_utf8_lossy(&command);
        command.to_mut().make_ascii_lowercase();
        dbg!(&command);
        if command.contains("ping") {
            stream.write_all(b"+PONG\r\n")?
        }
    }
    Ok(())
}
