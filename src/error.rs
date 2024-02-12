use std::io;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    ParseError(String),
    ValidationError(String),
    StateError(String),
}

pub type Result<T> = std::result::Result<T, Error>;

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
