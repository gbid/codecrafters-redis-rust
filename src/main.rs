use std::net::{ TcpListener, TcpStream };
use std::io::{ self, Read, Write };
use std::thread;
use std::str::FromStr;
use std::collections::HashMap;
use std::sync::{ Arc, Mutex };
use std::time::{ SystemTime, Duration };
use std::ops::Add;
use std::fmt;

#[derive(Debug)]
enum Error {
    Io(io::Error),
    ParseError(String),
    ValidationError(String),
    StateError(String),
}

type Result<T> = std::result::Result<T, Error>;

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Error {
        Error::ValidationError(format!("Invalid UTF-8 sequence: {}", err))
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(io_err) => write!(f, "IO error: {}", io_err),
            Error::ParseError(reason) => write!(f, "Parse error: {}", reason),
            Error::ValidationError(reason) => write!(f, "Validation error: {}", reason),
            Error::StateError(reason) => write!(f, "State error: {}", reason),
        }
    }
}

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;
    let map = Arc::new(Mutex::new(HashMap::new()));
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("accepted new connection");
                let map_arc = Arc::clone(&map);
                thread::spawn(move || {
                    if let Err(e) = handle_client_connection(&mut stream, map_arc) {
                        eprintln!("Failed to handle client connection: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Failed to establish TcpConnection: {}", e);
            }
        }
    }
    Ok(())
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
            return Err(Error::ParseError("Integer does not start with colon sign ':'".to_string()));
        }
        match raw[1] {
            b'+' => {
                let (num, raw_tail) = RespVal::parse_number(&raw[2..])?;
                Ok((RespVal::UnsignedInteger(num), raw_tail))
            },
            b'-' => {
                let (num, raw_tail) = RespVal::parse_number(&raw[2..])?;
                let negative_num: isize = -isize::try_from(num)
                    .map_err(|_| Error::ParseError(format!("Integer value is too large, max is {}", isize::MAX)))?;
                Ok((RespVal::SignedInteger(negative_num), raw_tail))
            },
            _ => {
                Err(Error::ParseError("Integer does not have sign prefix '+' or '-'.".to_string()))
            },
        }
    }

    fn parse_bulk_string(raw: &[u8]) -> Result<(RespVal, &[u8])> {
        if raw[0] != b'$' {
            return Err(Error::ParseError("Argument length does not start with dollar sign '$'".to_string()));
        }
        let (length, raw_tail) = RespVal::parse_number(&raw[1..])?;
        let raw_tail = raw_tail.strip_prefix(CRLF.as_ref())
            .ok_or_else(|| Error::ParseError("Error parsing Bulk String: length was not followed by CRLF sequence \r\n".to_string()))?;
        let bulk_string = RespVal::BulkString(raw_tail[0..length].to_vec());
        let raw_tail = raw_tail[length..].strip_prefix(CRLF.as_ref())
            .ok_or_else(|| Error::ParseError("Error parsing Bulk String: string was not followed by CRLF sequence \r\n".to_string()))?;
        Ok((bulk_string, &raw_tail))
    }

    fn parse_resp_value(raw: &[u8]) -> Result<(RespVal, &[u8])> {
        match raw[0] {
            b'*' => Ok(RespVal::parse_array(raw)?),
            b'$' => Ok(RespVal::parse_bulk_string(raw)?),
            b':' => Ok(RespVal::parse_integer(raw)?),
            b'+' => Ok(RespVal::parse_simple_string(raw)?),
            _ => Err(Error::ParseError(format!("Leading byte {} does not correspond to a RESP type", raw[0])))
        }
    }

    fn parse_simple_string(raw: &[u8]) -> Result<(RespVal, &[u8])> {
        if raw[0] != b'+' {
            return Err(Error::ParseError("Simple String did not start with +".to_string()));
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
                        return Err(Error::ParseError("Carriage return byte \r without succeeding \n appeared within Simple String, which is not allowed".to_string()));
                    }
                },
                Some(b'\n') => {
                    return Err(Error::ParseError("Newline byte \n appeared within Simple String, which is not allowed".to_string()));
                },
                Some(&allowed_byte) => simple_string_content.push(allowed_byte),
                None => {
                    return Err(Error::ParseError("Simple string did not end with CRLF sequence \r\n".to_string()));
                }
            }
            end += 1;
        }
        let simple_string = RespVal::SimpleString(simple_string_content);
        Ok((simple_string, &raw[end..]))
    }

    fn parse_array(raw: &[u8]) -> Result<(RespVal, &[u8])> {
        if raw[0] != b'*' {
            return Err(Error::ParseError("Array did not start with *".to_string()));
        }
        let (length, raw_tail) = RespVal::parse_number(&raw[1..])?;
        let mut raw_tail = raw_tail.strip_prefix(CRLF.as_ref())
            .ok_or_else(|| Error::ParseError("Error parsing Array: length was not followed by CRLF sequence \r\n".to_string()))?;

        let mut array = Vec::with_capacity(length);
        for _i in 0..length  {
            let (val, new_raw_tail) = RespVal::parse_resp_value(raw_tail)?;
            array.push(val);
            raw_tail = new_raw_tail;
        }
        let resp_array = RespVal::Array(array);
        Ok((resp_array, &raw_tail))
    }

    fn parse_number(raw: &[u8]) -> Result<(usize, &[u8])> {
        dbg!(String::from_utf8_lossy(raw));
        let mut end = 0;
        while let Some(byte) = raw.get(end) {
            if byte.is_ascii_digit() {
                end += 1;
            }
            else {
                break;
            }
        }
        let string = String::from_utf8(raw[0..end].to_vec())?;
        dbg!(&string);
        let number = usize::from_str(&string)
            .map_err(|err| Error::ParseError(format!("Error parsing number: {}", err)))?;
        Ok((number, &raw[end..]))
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
        let (parts, _): (RespVal, _)  = RespVal::parse_array(raw)?;
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
                                    Err(Error::ValidationError("ECHO requires an argument. This argument must be a BulkString.".to_string()))
                                }
                            },
                            b"set" => {
                                let args = &vals[1..];
                                RedisCommand::parse_set_args(args)
                            },
                            b"get" => {
                                let args = &vals[1..];
                                RedisCommand::parse_get_args(args)
                            },
                            _ =>
                                 Err(Error::ValidationError(format!("Unknown Command {}", String::from_utf8_lossy(command_bytes)))),
                        }
                    }
                    _ =>
                        Err(Error::ValidationError("No command provided as Bulk String".to_string())),
                }
            },
            _ => Err(Error::ValidationError(format!("Can parse Redis Command only from RESP Array, but got {:?}", parts))),
        }
    }

    fn parse_set_args(args: &[RespVal]) -> Result<RedisCommand> {
        if args.len() < 2 {
            return Err(Error::ValidationError("SET command requires at least two arguments".to_string()));
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
            _ => Err(Error::ValidationError("The first two arguments of SET must be Bulk Strings".to_string()))
        }
    }

    fn parse_get_args(args: &[RespVal]) -> Result<RedisCommand> {
        match args.get(0) {
            Some(RespVal::BulkString(key)) => {
                Ok(RedisCommand::Get(key.clone()))
            },
            _ => {
                Err(Error::ValidationError("GET command requires a Bulk String as first argument.".to_string()))
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
        if args.is_empty() {
            return Err(Error::ValidationError("No Option given".to_string()));
        }
        match &args[0] {
            RespVal::BulkString(arg1) if arg1.eq_ignore_ascii_case(b"px") => {
                let px_arg_error = Error::ValidationError("Option 'px' was not followed by unsigned integer".to_string());
                match &args[1] {
                    RespVal::BulkString(arg2) => {
                        let arg2 = String::from_utf8(arg2.to_vec())?;
                        let period_of_validity =
                            u64::from_str(&arg2)
                            .map_err(|_| px_arg_error)?;
                        Ok(SetOption::Px(period_of_validity))
                    },
                    _ => Err(px_arg_error),
                }
            },
            _ => Err(Error::ValidationError("Provided unknown Option for SET command".to_string())),
        }
    }
}

fn encode_as_bulk_string(bytes: &[u8]) -> Vec<u8> {
    // 15 comes from approximation of max byte length of string representation of bytes.len()
    let mut result = Vec::with_capacity(bytes.len() + 15);
    write!(result, "${}\r\n", bytes.len()).expect("Failed to write length prefix");
    result.extend_from_slice(bytes);
    result.extend_from_slice(b"\r\n");

    result
}

fn handle_client_connection(stream: &mut TcpStream, map: Arc<Mutex<HashMap<Vec<u8>, Value>>>) -> Result<()> {
    loop {
        let mut buffer: Vec<u8> = vec![0; 1024];
        let bytes_read = stream.read(&mut buffer)?;
        buffer.truncate(bytes_read);
        dbg!(String::from_utf8_lossy(&buffer));
        let command: RedisCommand = RedisCommand::parse_command(&buffer)?;
        let mut map = map.lock()
            .map_err(|_| Error::StateError("Mutex lock failed".to_string()))?;
        dbg!(&command);
        match command {
            RedisCommand::Ping => {
                let response = b"+PONG\r\n";
                stream.write_all(response)?;
            },
            RedisCommand::Echo(bytes) => {
                let response = encode_as_bulk_string(&bytes);
                dbg!(String::from_utf8_lossy(&response));
                stream.write_all(&response)?;
            },
            RedisCommand::Get(key_bytes) => {
                let null = b"$-1\r\n";
                let value: Option<Vec<u8>> = map
                    .get(&key_bytes)
                    .and_then(|value| {
                        let my_value = value.clone();
                        my_value.move_out_data_if_valid()
                    });
                let value_bulk_string = value.map(|data| encode_as_bulk_string(&data));
                match value_bulk_string {
                    Some(val) => stream.write_all(&val)?,
                    None => stream.write_all(null)?,
                };
            },
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
                    _ => None
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
