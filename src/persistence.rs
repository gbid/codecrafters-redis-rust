use crate::Value;
use crate::error::{ Result, Error };
use std::io::Read;
use std::fs::File;
use std::path::Path;
use std::collections::HashMap;
use nom::{
    IResult,
    combinator::{ value },
    bytes::complete as bytes,
    number::complete::{ le_u8, le_u16, le_u32, le_u64 },
    branch::alt,
    sequence::tuple,
};


type Database = HashMap<Vec<u8>, Value>;

pub fn load_rdb_file(rdb_file_path: &Path) -> Result<Database> {
    let mut file = File::open(rdb_file_path)?;
    let mut bytes: Vec<u8> = vec![];
    dbg!(rdb_file_path);
    file.read_to_end(&mut bytes).unwrap();
    // dbg!(HexSlice(&bytes));
    let (_remaining_bytes, database) = parse_rdb(&bytes)
        .map_err(|e| Error::RdbError(format!("Failed to parse RDB file: {}", e)))?;
    Ok(database)
}

fn parse_rdb(input: &[u8]) -> IResult<&[u8], Database> {
    todo!()
}

#[derive(Clone, Copy)]
struct RedisMagicNumber;
fn parse_magic_number(input: &[u8]) -> IResult<&[u8], RedisMagicNumber> {
    value(RedisMagicNumber, bytes::tag(b"REDIS"))
        (input)
}

#[derive(Clone, Copy)]
enum RdbVersion {
    V0001,
    V0002,
    V0003,
    V0004,
    V0005,
}

fn parse_rdb_version(input: &[u8]) -> IResult<&[u8], RdbVersion> {
    alt((
        value(RdbVersion::V0001, bytes::tag(b"0001")),
        value(RdbVersion::V0002, bytes::tag(b"0002")),
        value(RdbVersion::V0003, bytes::tag(b"0003")),
        value(RdbVersion::V0004, bytes::tag(b"0004")),
        value(RdbVersion::V0005, bytes::tag(b"0005")),
    ))
    (input)
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
    fn parse(input: &[u8]) -> IResult<&[u8], Opcode> {
        alt((
                value(Opcode::Eof, bytes::tag(&[0xFF])),
                value(Opcode::SelectDb, bytes::tag(&[0xFE])),
                value(Opcode::ExpireTime, bytes::tag(&[0xFD])),
                value(Opcode::ExpireTimeMS, bytes::tag(&[0xFC])),
                value(Opcode::ResizeDb, bytes::tag(&[0xFB])),
                value(Opcode::Aux, bytes::tag(&[0xFA])),
        ))
            (input)
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

    fn parse(input: &[u8]) -> IResult<&[u8], RdbValueType> {
        value(RdbValueType::StringEncoding, bytes::tag(b"\0"))
            (input)
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
    fn parse_expire_time(input: &[u8]) -> IResult<&[u8], Operation> {
        let (input, expires_in) = le_u32(input)?;
        let expires_in_millis = u64::from(expires_in) * 1000;
        Operation::parse_entry_after_expiry(input, Some(expires_in_millis))
    }
    fn parse_expire_time_ms(input: &[u8]) -> IResult<&[u8], Operation> {
        let (input, expires_in) = le_u64(input)?;
        Operation::parse_entry_after_expiry(input, Some(expires_in))
    }
    fn parse_nonexpire_entry(input: &[u8]) -> IResult<&[u8], Operation> {
        Operation::parse_entry_after_expiry(input, None)
    }
    fn parse_entry_after_expiry(input: &[u8], expires_in: Option<u64>) -> IResult<&[u8], Operation>{
        let (input, value_type) = RdbValueType::parse(input)?;
        let (input, key) = parse_string_encoding(input)?;
        let (input, val) = parse_value(input, value_type)?;
        Ok((input, entry_from_key_val(key, val, None)))
    }
    fn parse_resize_db(input: &[u8]) -> IResult<&[u8], Operation> {
        let (input, (size_nonexpire_hashtable, size_expire_hashtable)) = tuple((
                parse_length,
                parse_length
        ))(input)?;
        match (size_nonexpire_hashtable, size_expire_hashtable) {
            (Length::Simple(size_nonexpire_hashtable), Length::Simple(size_expire_hashtable)) =>
                Ok((input, Operation::ResizeDb(size_nonexpire_hashtable, size_expire_hashtable))),
            _ =>
                Err(nom::Err::Error(nom::error::make_error(input, nom::error::ErrorKind::Tag))),
        }
    }
    fn parse_auxiliary_field(input: &[u8]) -> IResult<&[u8], Operation> {
        let (input, key) = parse_string_encoding(input)?;
        match key {
            StringEncoding::Integer(num) =>
                Err(nom::Err::Error(nom::error::make_error(input, nom::error::ErrorKind::Tag))),
            StringEncoding::String(key_string) => {
                dbg!(String::from_utf8_lossy(&key_string));
                let (input, val) = parse_string_encoding(input)?;
                // dbg!(String::from_utf8_lossy(&val));
                // dbg!(HexSlice(&bytes));
                Ok((input, Operation::Aux(key_string, val)))
            }
        }
    }
    fn parse_select_db(input: &[u8]) -> IResult<&[u8], Operation> {
        let (input, db_number) = parse_length(input)?;
        let db_number = match db_number {
            Length::Simple(length) => length,
            Length::StringEncoding(length) => length,
        };
        Ok((input, Operation::SelectDb(db_number)))
    }
    fn parse_part<'a>(input: &'a[u8]) -> IResult<&'a[u8], Operation> {
        let opcode_parser = |input: &'a[u8]| -> IResult<&'a[u8], Operation> {
            let (input, opcode) = Opcode::parse(input)?;
            match opcode {
                Opcode::Eof => Ok((input, Operation::Eof)),
                Opcode::SelectDb => Operation::parse_select_db(input),
                Opcode::ExpireTime => Operation::parse_expire_time(input),
                Opcode::ExpireTimeMS => Operation::parse_expire_time_ms(input),
                Opcode::ResizeDb => Operation::parse_resize_db(input),
                Opcode::Aux => Operation::parse_auxiliary_field(input),
            }
        };
        alt((
            opcode_parser,
            Operation::parse_nonexpire_entry
        ))
            (input)
    }
}

fn parse_value(input: &[u8], value_type: RdbValueType) -> IResult<&[u8], RdbValue> {
    match value_type {
        RdbValueType::StringEncoding => {
            let (input, string_encoding) = parse_string_encoding(input)?;
            Ok((input, RdbValue::StringEncoding(string_encoding)))
        }
    }
}
fn parse_string_encoding(input: &[u8]) -> IResult<&[u8], StringEncoding> {
    let (input, length) = parse_length(input)?;
    match length {
        Length::Simple(length) => {
            let (input, data) = bytes::take(length)(input)?;
            Ok((input, StringEncoding::String(data.to_vec())))
        }
        Length::StringEncoding(num) => Ok((input, StringEncoding::Integer(num))),
    }
}
fn parse_length(input: &[u8]) -> IResult<&[u8], Length> {
    let (input, first_byte) = bytes::take(1usize)(input)?;
    let first_byte = first_byte[0];
    let upper_two_bits = first_byte & 0b00111111;
    let lower_six_bits = first_byte & 0b11000000;
    let (input, length) = match upper_two_bits {
        0 =>
            (input, Length::Simple(lower_six_bits.into())),
        1 => {
            let (input, length) = le_u16(input)?;
            (input, Length::Simple(length.into()))
        }
        2 => {
            let (input, length) = le_u32(input)?;
            (input, Length::Simple(length))
        }
        3 => match lower_six_bits {
            0 => {
                let (input, length) = le_u8(input)?;
                (input, Length::StringEncoding(length.into()))
            }
            1 => {
                let (input, length) = le_u16(input)?;
                (input, Length::StringEncoding(length.into()))
            }
            2 => {
                let (input, length) = le_u32(input)?;
                (input, Length::StringEncoding(length))
            }
            _ =>
                return Err(nom::Err::Error(nom::error::make_error(input, nom::error::ErrorKind::Tag)))
        }
        _ => unreachable!(),
    };
    Ok((input, length))
}

// TODO: implement other Value types
#[derive(PartialEq, Eq, Debug)]
enum RdbValue {
    StringEncoding(StringEncoding),
}

#[derive(Clone, Copy, Debug)]
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
        let bytes = b"\xFD\x05\x00\x00\x00\x00\x03key\x03val"; // 'FD' opcode, 5 seconds expiration, 'key', 'val'
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
        let bytes = b"\x80\x04\x00\x00\x00"; // Represents length 4 with encoding type 10 (32-bit length)
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
            b"\xFD\x0A\x00\x00\x00\x00\x04key1\x06value1".to_vec(), // Key with expiry
            b"\xFC\x0A\x00\x00\x00\x00\x00\x00\x00\x00\x04key2\x06value2".to_vec(), // Key with expiry
            b"\xFC\xE8\x03\x00\x00\x00\x00\x00\x00\x00\x04key3\x06value3".to_vec(), // Key with expiry
            b"\xFF".to_vec(),                // EOF
            b"\x00\x00\x00\x00\x00\x00\x00\x00".to_vec() // Mocked checksum (8 bytes, simplified)
        ].concat();

        // Parse the mock RDB content
        let result = parse_rdb(&rdb_content).unwrap();

        // Expected results
        let mut expected_db = HashMap::new();
        expected_db.insert(b"key1".to_vec(), Value::expiring_from_seconds(b"value1".to_vec(), 10));
        expected_db.insert(b"key2".to_vec(), Value::expiring_from_millis(b"value2".to_vec(), 10));
        expected_db.insert(b"key3".to_vec(), Value::expiring_from_seconds(b"value3".to_vec(), 1));

        // Assertion
        assert_eq!(result, expected_db);
    }
}
