use std::net::{ TcpListener, TcpStream };
use std::io::{ Result, Read, Write };
use std::thread;
use std::str::FromStr;
use std::collections::HashMap;
use std::sync::{ Arc, Mutex };

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    // let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let map = Arc::new(Mutex::new(HashMap::new()));
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let map_arc = Arc::clone(&map);
                thread::spawn(move || {
                    handle_client_connection(&mut stream, map_arc).unwrap();
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn parse_number(raw: &[u8]) -> Result<(usize, &[u8])> {
    dbg!(String::from_utf8(raw.to_vec()).unwrap());
    let mut end = 0;
    while let Some(byte) = raw.get(end) {
        if byte.is_ascii_digit() {
            end += 1;
        }
        else {
            break;
        }
    }
    // TODO: end == 0 ?
    let string = String::from_utf8(raw[0..end].to_vec()).unwrap();
    dbg!(&string);
    let number_of_arguments = usize::from_str(&string).unwrap();
    Ok((number_of_arguments, &raw[end..]))
}

fn parse_number_of_arguments(raw: &[u8]) -> Result<(usize, &[u8])> {
    if raw[0] != b'*' {
        panic!("Argument number does not start with asterix *");
    }
    parse_number(&raw[1..])
}

fn parse_argument_length(raw: &[u8]) -> Result<(usize, &[u8])> {
    dbg!(String::from_utf8(raw.to_vec()).unwrap());
    if raw[0] != b'$' {
        panic!("Argument length does not start with dollar sign $");
    }
    parse_number(&raw[1..])
}

#[derive(Debug)]
enum CommandKind {
    Ping,
    Echo,
    Set,
    Get,
}

#[derive(Debug)]
struct Command {
    kind: CommandKind,
    arguments: Vec<Vec<u8>>,
}

fn parse_command_kind(raw: &[u8], length: usize) -> Result<(CommandKind, &[u8])> {
    if raw.len() < length {
        panic!("CommandKind length larger than byte slice length")
    }

    let command_str = &raw[0..length].to_ascii_lowercase();
    let command_slice = &command_str[..]; // Convert Vec<u8> to &[u8]

    match command_slice {
        b"ping" => Ok((CommandKind::Ping, &raw[length..])),
        b"echo" => Ok((CommandKind::Echo, &raw[length..])),
        b"set" => Ok((CommandKind::Set, &raw[length..])),
        b"get" => Ok((CommandKind::Get, &raw[length..])),
        _ => panic!("Unkown CommandKind"),
    }
}

fn parse_command_argument(raw: &[u8], length: usize) -> Result<(Vec<u8>, &[u8])> {
    // TODO: raw.len() < length
    Ok((raw[0..length].to_vec(), &raw[length..]))
}

fn lines(bytes: &[u8]) -> Vec<&[u8]> {
    let mut parts = Vec::new();
    let mut start = 0;
    let crlf = b"\r\n";
    while let Some(position) = bytes[start..].windows(crlf.len()).position(|pair| pair == crlf) {
        let absolute_position = position + start;
        parts.push(&bytes[start..absolute_position]);
        start = absolute_position + 2;
    }
    parts
}

fn parse_command(raw: &[u8]) -> Result<Command> {
    let my_lines = lines(raw);
    for line in my_lines.iter() {
        dbg!(String::from_utf8(line.to_vec()).unwrap());
    }
    let mut lines = my_lines.iter();
    let (num, _) = parse_number_of_arguments(lines.next().unwrap())?;
    dbg!(num);
    let (kind_length, _) = parse_argument_length(lines.next().unwrap())?;
    dbg!(kind_length);
    let (kind, _) = parse_command_kind(lines.next().unwrap(), kind_length)?;
    dbg!(&kind);
    let num_of_arguments = num - 1;
    let mut arguments = Vec::with_capacity(num_of_arguments);
    for _i in 0..num_of_arguments {
        let (arg_length, _) = parse_argument_length(lines.next().unwrap())?;
        dbg!(arg_length);
        let (arg, _) = parse_command_argument(lines.next().unwrap(), arg_length)?;
        dbg!(String::from_utf8(arg.clone()).unwrap());
        arguments.push(arg);
    }
    let command = Command {
        kind,
        arguments,
    };
    Ok(command)
}

fn encode_as_bulk_string(bytes: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    result.push(b'$');
    for ch in bytes.len().to_string().chars() {
        result.push(ch.try_into().unwrap());
    }
    result.push(b'\r');
    result.push(b'\n');
    result.extend_from_slice(bytes);
    result.push(b'\r');
    result.push(b'\n');
    result
}
fn handle_client_connection(stream: &mut TcpStream, map: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>) -> Result<()> {
    loop {
        let mut buffer: Vec<u8> = vec![0; 1024];
        let bytes_read = stream.read(&mut buffer)?;
        buffer.truncate(bytes_read);
        dbg!(String::from_utf8(buffer.clone()).unwrap());
        let command = parse_command(&buffer)?;
        let mut map = map.lock().unwrap();
        dbg!(&command);
        match command.kind {
            CommandKind::Ping => {
                let response = b"+PONG\r\n";
                stream.write_all(response)?;
            },
            CommandKind::Echo => {
                for arg in command.arguments {
                    let response = encode_as_bulk_string(&arg);
                    dbg!(String::from_utf8(response.to_vec()).unwrap());
                    stream.write_all(&response)?;
                }
            }
            CommandKind::Get => {
                for arg in command.arguments {
                    let nil = b"(nil)";
                    let response =
                        map
                        .get(&arg)
                        .map_or(nil as &[u8], Vec::as_slice);
                    let response = encode_as_bulk_string(response);
                    stream.write_all(&response)?;
                }
            }
            CommandKind::Set => {
                let key = &command.arguments[0];
                let value = &command.arguments[1];
                map.insert(key.clone(), value.clone());
                let response = b"+OK\r\n";
                stream.write_all(response)?;
            }
        }
    }
}
