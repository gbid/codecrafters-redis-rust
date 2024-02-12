use crate::error::{ Error, Result };
use crate::{ CRLF };
use std::str::FromStr;
use std::io::Write;

#[derive(Debug)]
pub enum RespVal {
    BulkString(Vec<u8>),
    Array(Vec<RespVal>),
    SimpleString(Vec<u8>),
    UnsignedInteger(usize),
    SignedInteger(isize),
}

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

    pub fn parse_array(raw: &[u8]) -> Result<(RespVal, &[u8])> {
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

pub fn encode_as_bulk_string(bytes: &[u8]) -> Vec<u8> {
    // 15 comes from approximation of max byte length of string representation of bytes.len()
    let mut result = Vec::with_capacity(bytes.len() + 15);
    write!(result, "${}\r\n", bytes.len()).expect("Failed to write length prefix");
    result.extend_from_slice(bytes);
    result.extend_from_slice(b"\r\n");

    result
}
