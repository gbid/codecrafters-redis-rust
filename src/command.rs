use crate::error::{Error, Result};
use crate::resp::RespVal;
use std::str::FromStr;

#[derive(Debug)]
pub enum RedisCommand {
    Ping,
    Echo(Vec<u8>),
    Set(SetData),
    Get(Vec<u8>),
    ConfigGet(Vec<u8>),
}

impl RedisCommand {
    pub fn parse_command(raw: &[u8]) -> Result<RedisCommand> {
        let (parts, _): (RespVal, _) = RespVal::parse_array(raw)?;
        match parts {
            RespVal::Array(vals) => {
                match vals.get(0) {
                    Some(RespVal::BulkString(command_bytes)) => match command_bytes.as_slice() {
                        b"ping" => Ok(RedisCommand::Ping),
                        b"echo" => {
                            if let Some(RespVal::BulkString(arg_bytes)) = vals.get(1) {
                                Ok(RedisCommand::Echo(arg_bytes.clone()))
                            } else {
                                Err(Error::ValidationError("ECHO requires an argument. This argument must be a BulkString.".to_string()))
                            }
                        }
                        b"set" => {
                            let args = &vals[1..];
                            RedisCommand::parse_set_args(args)
                        }
                        b"get" => {
                            let args = &vals[1..];
                            RedisCommand::parse_get_args(args)
                        }
                        b"config" => {
                            let args = &vals[1..];
                            RedisCommand::parse_config_args(args)
                        }
                        _ => Err(Error::ValidationError(format!(
                            "Unknown Command {}",
                            String::from_utf8_lossy(command_bytes)
                        ))),
                    },
                    _ => Err(Error::ValidationError(
                        "No command provided as Bulk String".to_string(),
                    )),
                }
            }
            _ => Err(Error::ValidationError(format!(
                "Can parse Redis Command only from RESP Array, but got {:?}",
                parts
            ))),
        }
    }

    fn parse_set_args(args: &[RespVal]) -> Result<RedisCommand> {
        if args.len() < 2 {
            return Err(Error::ValidationError(
                "SET command requires at least two arguments".to_string(),
            ));
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
            }
            _ => Err(Error::ValidationError(
                "The first two arguments of SET must be Bulk Strings".to_string(),
            )),
        }
    }

    fn parse_get_args(args: &[RespVal]) -> Result<RedisCommand> {
        match args.get(0) {
            Some(RespVal::BulkString(key)) => Ok(RedisCommand::Get(key.clone())),
            _ => Err(Error::ValidationError(
                "GET command requires a Bulk String as first argument.".to_string(),
            )),
        }
    }

    fn parse_config_args(args: &[RespVal]) -> Result<RedisCommand> {
        if args.len() < 2 {
            return Err(Error::ValidationError(
                "CONFIG command requires two Bulk Strings first arguments".to_string()));
        }
        match &args[0] {
            RespVal::BulkString(subcommand) if subcommand.as_slice() == b"get" => {
                match &args[1] {
                    RespVal::BulkString(key) => Ok(RedisCommand::ConfigGet(key.clone())),
                    _ => Err(Error::ValidationError(
                            "CONFIG GET command requires a Bulk String as argument.".to_string(),
                    )),
                }
            }
            _ => Err(Error::ValidationError(
                    "CONFIG command requires a Bulk String with Subcommand[get| ] as first argument.".to_string(),
            )),
        }
    }
}

#[derive(Debug)]
pub struct SetData {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub options: Vec<SetOption>,
}

#[derive(Debug)]
pub enum SetOption {
    Px(u64),
}

impl SetOption {
    fn parse_from(args: &[RespVal]) -> Result<SetOption> {
        if args.is_empty() {
            return Err(Error::ValidationError("No Option given".to_string()));
        }
        match &args[0] {
            RespVal::BulkString(arg1) if arg1.eq_ignore_ascii_case(b"px") => {
                let px_arg_error = Error::ValidationError(
                    "Option 'px' was not followed by unsigned integer".to_string(),
                );
                match &args[1] {
                    RespVal::BulkString(arg2) => {
                        let arg2 = String::from_utf8(arg2.to_vec())?;
                        let period_of_validity = u64::from_str(&arg2).map_err(|_| px_arg_error)?;
                        Ok(SetOption::Px(period_of_validity))
                    }
                    _ => Err(px_arg_error),
                }
            }
            _ => Err(Error::ValidationError(
                "Provided unknown Option for SET command".to_string(),
            )),
        }
    }
}
