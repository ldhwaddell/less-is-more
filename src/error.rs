use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct InvalidLineError;

impl fmt::Display for InvalidLineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid line format")
    }
}

impl Error for InvalidLineError {}
