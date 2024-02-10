use std::net::{ TcpListener, TcpStream };
use std::io::{ Result, Read, Write, Error, ErrorKind };
use std::thread;
use std::str::FromStr;
use std::collections::HashMap;
use std::sync::{ Arc, Mutex };
use std::time::{ SystemTime, Duration };
use std::ops::Add;

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

#[derive(Clone)]
struct Value {
    data: Vec<u8>,
    expiration_time: Option<SystemTime>,
}

impl Value {
    fn move_out_data_if_valid(self) -> Option<Vec<u8>> {
        // TODO: use Option::take_if once it is in stable Rust
        match self.expiration_time {
            None  => Some(self.data),
            Some(expiration_time)
                if expiration_time > SystemTime::now()
                    => Some(self.data),
            _ => None,
        }
    }
}


#[derive(Debug)]
enum RespVal {
    BulkString(Vec<u8>),
    Array(Vec<RespVal>),
    SimpleString(Vec<u8>),
    UnsignedInteger(usize),
    SignedInteger(isize),
}

const CRLF: [u8; 2] = [b'\r',b'\n'];
impl RespVal {
    fn parse_integer(raw: &[u8]) -> Result<(RespVal, &[u8])> {
        if raw[0] != b':' {
            panic!("Integer does not start with colon sign ':'");
        }
        match raw[1] {
            b'+' => {
                let (num, raw_tail) = RespVal::parse_number(&raw[2..])?;
                Ok((RespVal::UnsignedInteger(num), raw_tail))
            },
            b'-' => {
                let (num, raw_tail) = RespVal::parse_number(&raw[2..])?;
                let negative_num: isize = -1 * isize::try_from(num).unwrap();
                Ok((RespVal::SignedInteger(negative_num), raw_tail))
            },
            _ => {
                panic!("Integer does not have sign prefix '+' or '-'");
            },
        }
    }

    fn parse_bulk_string(raw: &[u8]) -> Result<(RespVal, &[u8])> {
        if raw[0] != b'$' {
            panic!("Argument length does not start with dollar sign $");
        }
        let (length, raw_tail) = RespVal::parse_number(&raw[1..])?;
        
        let raw_tail = raw_tail.strip_prefix(CRLF.as_ref()).unwrap();
        let bulk_string = RespVal::BulkString(raw_tail[0..length].to_vec());
        let raw_tail = raw_tail[length..].strip_prefix(CRLF.as_ref()).unwrap();
        Ok((bulk_string, &raw_tail))
    }

    fn parse_resp_value(raw: &[u8]) -> Result<(RespVal, &[u8])> {
        let val = match raw[0] {
            b'*' => RespVal::parse_array(raw)?,
            b'$' => RespVal::parse_bulk_string(raw)?,
            b':' => RespVal::parse_integer(raw)?,
            b'+' => RespVal::parse_simple_string(raw)?,
            _ => panic!("Unkown RESP type"),
        };
        Ok(val)
    }

    fn parse_simple_string(raw: &[u8]) -> Result<(RespVal, &[u8])> {
        if raw[0] != b'+' {
            panic!("Simple String does not start with +");
        }
        let mut simple_string_content: Vec<u8> = Vec::new();
        let mut end = 0;
        loop {
            let byte = raw.get(end);
            match byte {
                Some(b'\r') => {
                    if let Some(b'\n') = raw.get(end+1) {
                        break;
                    }
                    else {
                        panic!("Carriage return byte \r without succeeding \n appeared within Simple String, which is not allowed");
                    }
                },
                Some(b'\n') => {
                    panic!("Newline byte \n appeared within Simple String, which is not allowed");
                },
                Some(&allowed_byte) => simple_string_content.push(allowed_byte),
                None => panic!("Simple string did not end with CRLF sequence \r\n"),
            }
            end += 1;
        }
        let simple_string = RespVal::SimpleString(simple_string_content);
        Ok((simple_string, &raw[end..]))
    }

    fn parse_array(raw: &[u8]) -> Result<(RespVal, &[u8])> {
        if raw[0] != b'*' {
            panic!("Array does not start with *");
        }
        let (length, raw_tail) = RespVal::parse_number(&raw[1..])?;
        let mut raw_tail = raw_tail.strip_prefix(CRLF.as_ref()).unwrap();

        let mut array = Vec::with_capacity(length);
        for _i in 0..length  {
            let (val, new_raw_tail) = RespVal::parse_resp_value(&raw_tail)?;
            array.push(val);
            raw_tail = new_raw_tail;
        }
        let resp_array = RespVal::Array(array);
        Ok((resp_array, &raw_tail))
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
}

#[derive(Debug)]
enum RedisCommand {
    Ping,
    Echo(Vec<u8>),
    Set(SetData),
    Get(Vec<u8>),
}

impl RedisCommand {
    fn parse_command(raw: &[u8]) -> Result<RedisCommand> {
        let parts: RespVal  = RespVal::parse_array(raw)?.0;
        match parts {
            RespVal::Array(vals) => {
                match vals.get(0) {
                    Some(RespVal::BulkString(command_bytes)) => {
                        match command_bytes.as_slice() {
                            b"ping" => Ok(RedisCommand::Ping),
                            b"echo" => {
                                if let Some(RespVal::BulkString(arg_bytes)) = vals.get(1) {
                                    Ok(RedisCommand::Echo(arg_bytes.clone()))
                                }
                                else {
                                    panic!("ECHO requires an argument. This argument must be a BulkString.")
                                }
                            },
                            b"set" => {
                                let args = &vals[1..];
                                RedisCommand::parse_set_args(args)
                            }
                            b"get" => {
                                let args = &vals[1..];
                                RedisCommand::parse_get_args(args)
                            }
                            _ => panic!("Unkown Command"),
                        }
                    }
                    _ => panic!("No Command provided as Bulk String"),
                }
            },
            _ => panic!("RespVal::parse_array always returns RespVal::Array"),
        }
    }

    fn parse_set_args(args: &[RespVal]) -> Result<RedisCommand> {
        if args.len() < 2 {
            panic!("SET command requires at least two arguments");
        }
        match (&args[0], &args[1]) {
            (RespVal::BulkString(key), RespVal::BulkString(value)) => {
                let option = SetOption::parse_from(&args[2..]);
                let options = match option {
                    Ok(opt) => vec![opt],
                    _ => Vec::new(),
                };
                let set_data = SetData {
                    key: key.clone(),
                    value: value.clone(),
                    options,
                };
                Ok(RedisCommand::Set(set_data))
            },
            _ => {
                panic!("The first two arguments of SET must be Bulk Strings");
            },
        }
    }

    fn parse_get_args(args: &[RespVal]) -> Result<RedisCommand> {
        match args.get(0) {
            Some(RespVal::BulkString(key)) => {
                Ok(RedisCommand::Get(key.clone()))
            },
            _ => {
                panic!("GET command requires a Bulk String as first argument.")
            },
        }
    }
}

#[derive(Debug)]
struct SetData {
    key: Vec<u8>,
    value: Vec<u8>,
    options: Vec<SetOption>,
}

#[derive(Debug)]
enum SetOption {
    Px(u64),
}
impl SetOption {
    fn parse_from(args: &[RespVal]) -> Result<SetOption> {
        if args.len() == 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "No Options given."))
        }
        match &args[0] {
            RespVal::BulkString(arg1) if arg1.clone().to_ascii_lowercase().as_slice() == b"px" => {
                match &args[1] {
                    RespVal::BulkString(arg2) => {
                        let arg2 = String::from_utf8(arg2.to_vec()).unwrap();
                        Ok(SetOption::Px(u64::from_str(&arg2).unwrap()))
                    },
                    _ => {
                        return Err(Error::new(ErrorKind::InvalidInput, "Option 'px' was not followed by unsigned integer"))
                    },
                }
            },
            _ => return Err(Error::new(ErrorKind::InvalidInput, "Provided unkown Option for SET command"))
        }
    }
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
fn handle_client_connection(stream: &mut TcpStream, map: Arc<Mutex<HashMap<Vec<u8>, Value>>>) -> Result<()> {
    loop {
        let mut buffer: Vec<u8> = vec![0; 1024];
        let bytes_read = stream.read(&mut buffer)?;
        buffer.truncate(bytes_read);
        dbg!(String::from_utf8(buffer.clone()).unwrap());
        let command: RedisCommand = RedisCommand::parse_command(&buffer)?;
        let mut map = map.lock().unwrap();
        dbg!(&command);
        match command {
            RedisCommand::Ping => {
                let response = b"+PONG\r\n";
                stream.write_all(response)?;
            },
            RedisCommand::Echo(bytes) => {
                let response = encode_as_bulk_string(&bytes);
                dbg!(String::from_utf8(response.to_vec()).unwrap());
                stream.write_all(&response)?;
            },
            RedisCommand::Get(key_bytes) => {
                let nil = b"(nil)";
                let value: Option<Vec<u8>> = map
                    .get(&key_bytes)
                    .and_then(|value| {
                        let my_value = value.clone();
                        my_value.move_out_data_if_valid()
                    });
                let response = value.unwrap_or_else(|| nil.to_vec());
                let response = encode_as_bulk_string(&response);
                stream.write_all(&response)?;
            },
            RedisCommand::Set(SetData {
                key,
                value,
                options,
            }) => {
                let expiration_time = match options.get(0) {
                    Some(SetOption::Px(period_of_validity)) => {
                        let period_of_validity: u64 = (*period_of_validity).try_into().unwrap();
                        let period_of_validity = Duration::from_millis(period_of_validity);
                        let expiration_time = SystemTime::now().add(period_of_validity);
                        Some(expiration_time)
                    }
                    _ => {
                        None
                    }
                };
                let value = Value {
                    data: value,
                    expiration_time,
                };
                map.insert(key, value);
                let response = b"+OK\r\n";
                stream.write_all(response)?;
            },
        }
    }
}
