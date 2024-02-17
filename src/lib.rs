use crate::command::{RedisCommand, SetData, SetOption};
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Add;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use clap::Parser;
use std::path::PathBuf;

mod command;
mod error;
mod resp;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Config {
    #[arg(long)]
    dir: PathBuf,
    #[arg(long)]
    dbfilename: PathBuf,
}

#[derive(Clone)]
struct Value {
    data: Vec<u8>,
    expiration_time: Option<SystemTime>,
}

impl Value {
    fn move_out_data_if_valid(self) -> Option<Vec<u8>> {
        // TODO: use Option::take_if once it is in stable Rust
        match self.expiration_time {
            None => Some(self.data),
            Some(expiration_time) if expiration_time > SystemTime::now() => Some(self.data),
            _ => None,
        }
    }
}

const CRLF: [u8; 2] = [b'\r', b'\n'];

fn handle_client_connection(
    stream: &mut TcpStream,
    map: Arc<Mutex<HashMap<Vec<u8>, Value>>>,
    config: Config,
) -> Result<()> {
    loop {
        let mut buffer: Vec<u8> = vec![0; 1024];
        let bytes_read = stream.read(&mut buffer)?;
        buffer.truncate(bytes_read);
        dbg!(String::from_utf8_lossy(&buffer));
        let command: RedisCommand = RedisCommand::parse_command(&buffer)?;
        let mut map = map
            .lock()
            .map_err(|_| Error::StateError("Mutex lock failed".to_string()))?;
        dbg!(&command);
        match command {
            RedisCommand::Ping => {
                let response = b"+PONG\r\n";
                stream.write_all(response)?;
            }
            RedisCommand::Echo(bytes) => {
                let response = resp::encode_as_bulk_string(&bytes);
                dbg!(String::from_utf8_lossy(&response));
                stream.write_all(&response)?;
            }
            RedisCommand::Get(key_bytes) => {
                let null = b"$-1\r\n";
                let value: Option<Vec<u8>> = map.get(&key_bytes).and_then(|value| {
                    let my_value = value.clone();
                    my_value.move_out_data_if_valid()
                });
                let value_bulk_string = value.map(|data| resp::encode_as_bulk_string(&data));
                match value_bulk_string {
                    Some(val) => stream.write_all(&val)?,
                    None => stream.write_all(null)?,
                };
            }
            RedisCommand::Set(SetData {
                key,
                value,
                options,
            }) => {
                let expiration_time = match options.get(0) {
                    Some(SetOption::Px(period_of_validity)) => {
                        let period_of_validity = Duration::from_millis(*period_of_validity);
                        let expiration_time = SystemTime::now().add(period_of_validity);
                        Some(expiration_time)
                    }
                    _ => None,
                };
                let value = Value {
                    data: value,
                    expiration_time,
                };
                map.insert(key, value);
                let response = b"+OK\r\n";
                stream.write_all(response)?;
            }
            RedisCommand::ConfigGet(key) => {
                let val = match key.as_slice() {
                    b"dir" => &config.dir,
                    b"dbfilename" => &config.dbfilename,
                    _ => return Err(Error::ValidationError("Wrong argument to CONFIG GET command".to_string())),
                };
                use resp::RespVal;
                let key = resp::encode_as_bulk_string(&key);
                let key = RespVal::parse_resp_value(&key)?.0;
                let val = resp::encode_as_bulk_string(val.to_str().unwrap().as_bytes());
                let val = RespVal::parse_resp_value(&val)?.0;
                let response = resp::encode(&RespVal::Array(vec![key, val]));
                stream.write_all(&response)?;
            }
        }
    }
}

pub fn start_redis_server(socket_addr: SocketAddr, config: Config) {
    let listener = TcpListener::bind(socket_addr).expect("Failed to bind socket address");
    let map = Arc::new(Mutex::new(HashMap::new()));
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let map_arc = Arc::clone(&map);
                let my_config = config.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_client_connection(&mut stream, map_arc, my_config) {
                        eprintln!("Failed to handle client connection: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Failed to establish TcpConnection: {}", e);
            }
        }
    }
}
