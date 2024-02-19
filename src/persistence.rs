use crate::Value;
use crate::error::{ Result, Error };
use std::io::Read;
use std::fs::File;
use std::path::Path;
use std::collections::HashMap;

type Database = HashMap<Vec<u8>, Value>;

pub fn load_rdb_file(rdb_file_path: &Path) -> Result<Database> {
    let mut file = File::open(rdb_file_path)?;
    let mut bytes: Vec<u8> = vec![];
    dbg!(rdb_file_path);
    file.read_to_end(&mut bytes).unwrap();
    dbg!(HexSlice(&bytes));
    parse_rdb(&bytes)
}

fn parse_rdb(mut bytes: &[u8]) -> Result<Database> {
    // header
    bytes = parse_magic_number(&bytes)?;
    bytes = parse_rdb_version(&bytes)?;
    // parts
    let mut parts: Vec<Operation> = Vec::new();
    while !bytes.is_empty() {
        let (part, remaining_bytes) = parse_part(&bytes)?;
        parts.push(part);
        bytes = remaining_bytes;
    }
    let entries = parts.into_iter().filter_map(|part| match part {
        Operation::Entry(key, val) => Some((key, val)),
        _ => None,
    });
    Ok(HashMap::from_iter(entries))
}


fn parse_magic_number(bytes: &[u8]) -> Result<&[u8]> {
    let (magic_number, bytes) = bytes.split_at(5);
    if magic_number == b"REDIS" {
        Ok(bytes)
    } else {
        Err(Error::RdbError("RDB file did not start with Magic Number 'REDIS'".to_string()))
    }
}

fn parse_rdb_version(bytes: &[u8]) -> Result<&[u8]> {
    let (rdb_version, bytes) = bytes.split_at(4);
    if rdb_version == b"0003" {
        Ok(bytes)
    } else {
        Err(Error::RdbError("Encountered Unknown RDB version".to_string()))
    }
}

#[derive(Debug, Copy, Clone)]
enum Opcode {
    Eof,
    SelectDb,
    ExpireTime,
    ExpireTimeMS,
    ResizeDb,
    Aux,
}

impl Opcode {
    fn from_byte(byte: u8) -> Result<Opcode> {
        match byte {
            0xFF => Ok(Opcode::Eof),
            0xFE => Ok(Opcode::SelectDb),
            0xFD => Ok(Opcode::ExpireTime),
            0xFC => Ok(Opcode::ExpireTimeMS),
            0xFB => Ok(Opcode::ResizeDb),
            0xFA => Ok(Opcode::Aux),
            _ => Err(Error::RdbError("Encountered unknown opcode".to_string()))
        }
    }
}

#[derive(Debug)]
enum Operation {
    Eof,
    SelectDB(u32),
    Entry(Vec<u8>, Value),
    Aux(Vec<u8>, Vec<u8>),
}

use std::fmt;
struct HexSlice<'a>(&'a [u8]);
impl<'a> fmt::Debug for HexSlice<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[")?;
        for (count, byte) in self.0.iter().enumerate() {
            if count != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", byte)?;
        }
        write!(f, "]")
    }
}

fn parse_part(bytes: &[u8]) -> Result<(Operation, &[u8])> {
    let op = Opcode::from_byte(bytes[0]);
    dbg!(&op);
    dbg!(HexSlice(bytes));
    let operation = match op {
        Ok(Opcode::Eof) => Ok((Operation::Eof, &bytes[1..])),
        Ok(Opcode::SelectDb) => parse_select_db(&bytes[1..]),
        Ok(Opcode::ExpireTime) => parse_expire_time(&bytes[1..]),
        Ok(Opcode::ExpireTimeMS) => parse_expire_time_ms(&bytes[1..]),
        Ok(Opcode::ResizeDb) => parse_resize_db(&bytes[1..]),
        Ok(Opcode::Aux) => parse_auxiliary_field(&bytes[1..]),
        Err(_) => parse_nonexpire_entry(bytes),
    };
    dbg!(&operation);
    operation
}

fn parse_select_db(bytes: &[u8]) -> Result<(Operation, &[u8])> {
    let (db_number, bytes) = parse_length(bytes)?;
    Ok((Operation::SelectDB(db_number), bytes))
}

fn parse_expire_time(bytes: &[u8]) -> Result<(Operation, &[u8])> {
    let (expires_in_raw, bytes) = bytes.split_at(u32::BITS.try_into().unwrap());
    let expires_in = u32::from_be_bytes(expires_in_raw.try_into().unwrap());
    let (key, bytes) = parse_length_prefixed_string(bytes)?;
    let (val, bytes) = parse_value(bytes)?;
    let val = Value::expiring_from_seconds(val, expires_in);
    Ok((Operation::Entry(key, val), bytes))
}

fn parse_expire_time_ms(bytes: &[u8]) -> Result<(Operation, &[u8])> {
    let (expires_in_raw, bytes) = bytes.split_at(u64::BITS.try_into().unwrap());
    let expires_in = u64::from_be_bytes(expires_in_raw.try_into().unwrap());
    let (key, bytes) = parse_length_prefixed_string(bytes)?;
    let (val, bytes) = parse_value(bytes)?;
    let val = Value::expiring_from_millis(val, expires_in);
    Ok((Operation::Entry(key, val), bytes))
}

fn parse_nonexpire_entry(bytes: &[u8]) -> Result<(Operation, &[u8])> {
    let (key, bytes) = parse_length_prefixed_string(bytes)?;
    let (val, bytes) = parse_value(bytes)?;
    let val = Value {
        data: val,
        expiration_time: None,
    };
    Ok((Operation::Entry(key, val), bytes))
}

fn parse_resize_db(bytes: &[u8]) -> Result<(Operation, &[u8])> {
    todo!()
}

fn parse_auxiliary_field(bytes: &[u8]) -> Result<(Operation, &[u8])> {
    let (key, bytes) = parse_length_prefixed_string(bytes)?;
    dbg!(&key);
    dbg!(HexSlice(&bytes));
    let (val, bytes) = parse_length_prefixed_string(bytes)?;
    dbg!(&key);
    dbg!(HexSlice(&bytes));
    Ok((Operation::Aux(key, val), bytes))
}

fn parse_length_prefixed_string(bytes: &[u8]) -> Result<(Vec<u8>, &[u8])> {
    let (length, bytes) = parse_length(bytes)?;
    let (data, bytes) = bytes.split_at(length.try_into().unwrap());
    Ok((data.to_vec(), bytes))
}

fn parse_value(bytes: &[u8]) -> Result<(Vec<u8>, &[u8])> {
    match bytes[0] {
        0 => parse_length_prefixed_string(&bytes[1..]),
        _ => todo!(),
    }
}

fn parse_length(bytes: &[u8]) -> Result<(u32, &[u8])> {
    let first_byte = bytes[0] & 0b00111111;
    let msb = bytes[0] >> 6;
    match msb {
        0 => Ok((first_byte.into(), &bytes[1..])),
        1 => Ok((u16::from_be_bytes([first_byte, bytes[1]]).into(), &bytes[2..])),
        2 => Ok((u32::from_be_bytes(bytes[1..5].try_into().unwrap()), &bytes[5..])),
        3 => todo!("Special format not yet implemented"),
        _ => unreachable!(),
    }
}
