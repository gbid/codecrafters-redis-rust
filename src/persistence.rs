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
    // dbg!(HexSlice(&bytes));
    parse_rdb(&bytes)
}

fn parse_rdb(mut bytes: &[u8]) -> Result<Database> {
    // header
    dbg!(HexSlice(&bytes));
    bytes = parse_magic_number(&bytes)?;
    bytes = parse_rdb_version(&bytes)?;
    dbg!(HexSlice(&bytes));
    // parts
    let mut parts: Vec<Operation> = Vec::new();
    while !bytes.is_empty() {
        dbg!(HexSlice(&bytes));
        let (part, remaining_bytes) = Operation::parse_part(&bytes)?;
        parts.push(part);
        if let Some(Operation::Eof) = parts.last() {
            break;
        }
        if let Some(Operation::Entry(key, val)) = parts.last() {
            dbg!(String::from_utf8_lossy(&key));
            dbg!(String::from_utf8_lossy(&val.data));
            dbg!(val.expiration_time);
        }
        bytes = remaining_bytes;
    }
    dbg!(HexSlice(&bytes));
    let entries = parts.into_iter().filter_map(|part| match part {
        Operation::Entry(key, val) => {
            Some((key, val))
        }
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

#[derive(Debug, PartialEq, Eq)]
enum Operation {
    Eof,
    SelectDb(u32),
    Entry(Vec<u8>, Value),
    Aux(Vec<u8>, StringEncoding),
    ResizeDb(u32, u32)
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
            write!(f, "{:02x}", byte)?;
        }
        write!(f, "]")
    }
}


impl RdbValueType {
    fn from_byte(byte: u8) -> Result<RdbValueType> {
        match byte {
            0 => Ok(RdbValueType::StringEncoding),
            _ => Err(Error::RdbError("Encountered unkown value type.".to_string())),
        }
    }
}
fn entry_from_key_val(key: StringEncoding, val: RdbValue, expires_in: Option<u64>) -> Operation {
    let key_raw = match key {
        StringEncoding::String(key_raw) => key_raw,
        StringEncoding::Integer(_num) =>
            unimplemented!("String Encoded Integers not implemented as keys yet"),
    };
    let entry = match val {
        RdbValue::StringEncoding(StringEncoding::String(val_raw)) => {
            let redis_val = match expires_in {
                Some(expires_in) => Value::expiring_from_millis(val_raw, expires_in),
                None => Value {
                    data: val_raw,
                    expiration_time: None,
                },
            };
            Operation::Entry(key_raw, redis_val)
        }
        RdbValue::StringEncoding(StringEncoding::Integer(_num)) => {
            unimplemented!("String Encoded Integers not implemented as Value yet");
        }
    };
    entry
}
impl Operation {
    fn parse_expire_time(bytes: &[u8]) -> Result<(Operation, &[u8])> {
        let (expires_in_raw, bytes) = bytes.split_at(4);
        let expires_in = u32::from_be_bytes(expires_in_raw.try_into().unwrap());
        dbg!(&expires_in);
        let value_type = RdbValueType::from_byte(bytes[0])?;
        let bytes = &bytes[1..];
        let (key, bytes) = parse_string_encoding(bytes)?;
        let (rdb_val, bytes) = parse_value(bytes, value_type)?;
        let entry = entry_from_key_val(key, rdb_val, Some(u64::from(expires_in)*1000));
        Ok((entry, bytes))
    }

    fn parse_expire_time_ms(bytes: &[u8]) -> Result<(Operation, &[u8])> {
        let (expires_in_raw, bytes) = bytes.split_at(8);
        let expires_in = u64::from_be_bytes(expires_in_raw.try_into().unwrap());
        // dbg!(expires_in);
        let value_type = RdbValueType::from_byte(bytes[0])?;
        let bytes = &bytes[1..];
        let (key, bytes) = parse_string_encoding(bytes)?;
        let (val, bytes) = parse_value(bytes, value_type)?;
        let entry = entry_from_key_val(key, val, Some(expires_in));
        // dbg!(&entry);
        Ok((entry, bytes))
    }

    fn parse_nonexpire_entry(bytes: &[u8]) -> Result<(Operation, &[u8])> {
        let value_type = RdbValueType::from_byte(bytes[0])?;
        let bytes = &bytes[1..];
        let (key, bytes) = parse_string_encoding(bytes)?;
        let (val, bytes) = parse_value(bytes, value_type)?;
        Ok((entry_from_key_val(key, val, None), bytes))
    }

    fn parse_resize_db(bytes: &[u8]) -> Result<(Operation, &[u8])> {
        let (size_nonexpire_hashtable, bytes) = parse_length(bytes)?;
        let (size_expire_hashtable, bytes) = parse_length(bytes)?;
        match (size_nonexpire_hashtable, size_expire_hashtable) {
            (Length::Simple(size_nonexpire_hashtable), Length::Simple(size_expire_hashtable)) =>
                Ok((Operation::ResizeDb(size_nonexpire_hashtable, size_expire_hashtable), bytes)),
            _ =>
                Err(Error::RdbError("Encountered String encoded Integer in ResizeDb Operation".to_string())),
        }
    }
    fn parse_auxiliary_field(bytes: &[u8]) -> Result<(Operation, &[u8])> {
        let (key, bytes) = parse_string_encoding(bytes)?;
        match key {
            StringEncoding::Integer(num) =>
                return Err(Error::RdbError(format!("Encountered String encoded Integer {} as auxiliary key.", num))),
            StringEncoding::String(key_string) => {
                dbg!(String::from_utf8_lossy(&key_string));
                // dbg!(HexSlice(&bytes));
                let (val, bytes) = parse_string_encoding(bytes)?;
                // dbg!(String::from_utf8_lossy(&val));
                // dbg!(HexSlice(&bytes));
                Ok((Operation::Aux(key_string, val), bytes))
            }
        }
    }
    fn parse_select_db(bytes: &[u8]) -> Result<(Operation, &[u8])> {
        let (db_number, bytes) = parse_length(bytes)?;
        let db_number = match db_number {
            Length::Simple(length) => length,
            Length::StringEncoding(length) => length,
        };
        Ok((Operation::SelectDb(db_number), bytes))
    }
    fn parse_part(bytes: &[u8]) -> Result<(Operation, &[u8])> {
        let op = Opcode::from_byte(bytes[0]);
        // dbg!(&op);
        // dbg!(HexSlice(bytes));
        let operation = match op {
            Ok(Opcode::Eof) => Ok((Operation::Eof, &bytes[1..])),
            Ok(Opcode::SelectDb) => Operation::parse_select_db(&bytes[1..]),
            Ok(Opcode::ExpireTime) => Operation::parse_expire_time(&bytes[1..]),
            Ok(Opcode::ExpireTimeMS) => Operation::parse_expire_time_ms(&bytes[1..]),
            Ok(Opcode::ResizeDb) => Operation::parse_resize_db(&bytes[1..]),
            Ok(Opcode::Aux) => Operation::parse_auxiliary_field(&bytes[1..]),
            Err(_) => Operation::parse_nonexpire_entry(bytes),
        };
        // dbg!(&operation);
        operation
    }
}

fn parse_value(bytes: &[u8], value_type: RdbValueType) -> Result<(RdbValue, &[u8])> {
    match value_type {
        RdbValueType::StringEncoding => {
            let (string_encoding, bytes) = parse_string_encoding(bytes)?;
            Ok((RdbValue::StringEncoding(string_encoding), bytes))
        }
    }
}

fn parse_string_encoding(bytes: &[u8]) -> Result<(StringEncoding, &[u8])> {
    let (length, bytes) = parse_length(bytes)?;
    match length {
        Length::Simple(length) => {
            let (data, bytes) = bytes.split_at(length.try_into().unwrap());
            Ok((StringEncoding::String(data.to_vec()), bytes))
        }
        Length::StringEncoding(num) => Ok((StringEncoding::Integer(num), bytes)),
    }
}

fn parse_length(bytes: &[u8]) -> Result<(Length, &[u8])> {
    let first_byte = bytes[0] & 0b00111111;
    let msb = bytes[0] >> 6;
    match msb {
        0 => Ok((Length::Simple(first_byte.into()), &bytes[1..])),
        1 => Ok((Length::Simple(u16::from_be_bytes([first_byte, bytes[1]]).into()), &bytes[2..])),
        2 => Ok((Length::Simple(u32::from_be_bytes(bytes[1..5].try_into().unwrap())), &bytes[5..])),
        3 => match first_byte {
            0 => Ok((Length::StringEncoding(bytes[1].into()), &bytes[2..])),
            1 => Ok((Length::StringEncoding(u16::from_be_bytes(bytes[1..3].try_into().unwrap()).into()), &bytes[3..])),
            2 => Ok((Length::StringEncoding(u32::from_be_bytes(bytes[1..5].try_into().unwrap())), &bytes[5..])),
            _ => Err(Error::RdbError(format!("String encoded integer has unkown prefix {:02x} in last 6 bits of first byte.'", first_byte)))
        },
        _ => unreachable!(),
    }
}


// TODO: implement other Value types
#[derive(PartialEq, Eq, Debug)]
enum RdbValue {
    StringEncoding(StringEncoding),
}

enum RdbValueType {
    StringEncoding,
    // TODO: the other value types
}

#[derive(Debug, PartialEq, Eq)]
enum StringEncoding {
    String(Vec<u8>),
    Integer(u32),
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum Length {
    Simple(u32),
    StringEncoding(u32),
}

impl Into<u32> for Length {
    fn into(self) -> u32 {
        match self {
            Length::Simple(num) => num,
            Length::StringEncoding(num) => num,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_magic_number_valid() {
        let bytes = b"REDIS0006remainderofthedata...";
        assert_eq!(parse_magic_number(bytes).unwrap(), &bytes[5..]);
    }

    #[test]
    fn test_parse_magic_number_invalid() {
        let bytes = b"WRONG0006remainderofthedata...";
        assert!(parse_magic_number(bytes).is_err());
    }

    #[test]
    fn test_parse_rdb_version_valid() {
        let bytes = b"0003extra_data_after_version";
        assert_eq!(parse_rdb_version(bytes).unwrap(), &bytes[4..]);
    }

    #[test]
    fn test_parse_rdb_version_invalid() {
        let bytes = b"9999extra_data_after_wrong_version";
        assert!(parse_rdb_version(bytes).is_err());
    }

    #[test]
    fn test_parse_select_db() {
        // FE 00 represents a SELECTDB opcode followed by a 0 database number (variable length integer)
        let bytes = b"\xFE\x00";
        let expected = (Operation::SelectDb(0), &b""[..]); // Assuming the operation and empty remainder
        assert_eq!(Operation::parse_part(bytes).unwrap(), expected);
    }

    #[test]
    fn test_parse_aux() {
        // FA followed by two length-prefixed strings
        let bytes = b"\xFA\x05redis\x055.0.0"; // 'FA' opcode, 'redis' key, '5.0.0' value
        let expected_key = b"redis".to_vec();
        let expected_value = StringEncoding::String(b"5.0.0".to_vec());
        let expected = (Operation::Aux(expected_key, expected_value), &b""[..]);
        assert_eq!(Operation::parse_part(bytes).unwrap(), expected);
    }

    #[test]
    fn test_parse_expire_time() {
        // FD followed by a 4-byte timestamp and then a key-value pair (simplified)
        let bytes = b"\xFD\x00\x00\x00\x05\x00\x03key\x03val"; // 'FD' opcode, 5 seconds expiration, 'key', 'val'
        let expected_key = b"key".to_vec();
        let expected_val = Value::expiring_from_seconds(b"val".to_vec(), 5);
        let expected = (Operation::Entry(expected_key, expected_val), &b""[..]);
        assert_eq!(Operation::parse_expire_time(&bytes[1..]).unwrap(), expected); // Skip first byte (opcode)
    }

    // #[test]
    // fn test_parse_resize_db() {
    //     // FB followed by two length-encoded sizes
    //     let bytes = b"\xFB\x03\x04"; // 'FB' opcode, size 3 for db and 4 for expires (simple example)
    //     let expected = (Operation::ResizeDb(3, 4), &b""[..]);
    //     assert_eq!(parse_resize_db(&bytes[1..]).unwrap(), expected); // Skip first byte (opcode)
    // }

    #[test]
    fn test_parse_auxiliary_field() {
        // FA followed by two length-prefixed strings
        let bytes = b"\xFA\x05redis\x07version\x053.2.0";
        let expected_key = b"redis".to_vec();
        let expected_value = StringEncoding::String(b"version".to_vec());
        let remaining = b"\x053.2.0"; // Simulate remaining data after parsing
        let expected = (Operation::Aux(expected_key, expected_value), remaining.as_slice());
        assert_eq!(Operation::parse_auxiliary_field(&bytes[1..]).unwrap(), expected);
    }

    #[test]
    fn test_parse_length_prefixed_string_simple() {
        let bytes = b"\x03abcRemainingData";
        let expected_str = StringEncoding::String(b"abc".to_vec());
        let expected_remaining = b"RemainingData";
        assert_eq!(parse_string_encoding(bytes).unwrap(), (expected_str, expected_remaining.as_slice()));
    }
    #[test]
    fn test_parse_string_value() {
        // Assuming '0' indicates a simple string type
        let bytes = b"\x03abcRemainingData";
        let expected_val = RdbValue::StringEncoding(StringEncoding::String(b"abc".to_vec()));
        let expected_remaining = b"RemainingData";
        assert_eq!(parse_value(bytes, RdbValueType::StringEncoding).unwrap(), (expected_val, expected_remaining.as_slice()));
    }
    #[test]
    fn test_parse_length() {
        // 10000000 00000000 00000000 00000100
        let bytes = b"\x80\x00\x00\x00\x04"; // Represents length 4 with encoding type 10 (32-bit length)
        assert_eq!(parse_length(bytes).unwrap(), (Length::Simple(4), &b""[..])); // Adjust based on actual function signature
    }

    #[test]
    fn test_parse_complete_rdb_file() {
        use std::collections::HashMap;
        // Construct a mock RDB file content
        // Format: <MAGIC><VERSION><AUX><DBSELECTOR><RESIZEDB><EXPIRETIME><KEY><VALUE><EOF>
        let rdb_content = [
            b"REDIS".to_vec(),                // Magic Number
            b"0003".to_vec(),                // Version - for example purposes
            b"\xFA\x03ver\x036.2".to_vec(),  // AUX field - version 6.2
            b"\xFE\x00".to_vec(),            // Select DB 0
                                             // 0100 0100 
            b"\xFB\x04\x04".to_vec(), // RESIZEDB (simplified)
            b"\xFD\x00\x00\x00\x0A\x00\x04key1\x06value1".to_vec(), // Key with expiry
            b"\xFC\x00\x00\x00\x00\x00\x00\x00\x0A\x00\x04key2\x06value2".to_vec(), // Key with expiry
            b"\xFF".to_vec(),                // EOF
            b"\x00\x00\x00\x00\x00\x00\x00\x00".to_vec() // Mocked checksum (8 bytes, simplified)
        ].concat();

        // Parse the mock RDB content
        let result = parse_rdb(&rdb_content).unwrap();

        // Expected results
        let mut expected_db = HashMap::new();
        expected_db.insert(b"key1".to_vec(), Value::expiring_from_seconds(b"value1".to_vec(), 10));
        expected_db.insert(b"key2".to_vec(), Value::expiring_from_millis(b"value2".to_vec(), 10));

        // Assertion
        assert_eq!(result, expected_db);
        assert!(false);
    }
}
