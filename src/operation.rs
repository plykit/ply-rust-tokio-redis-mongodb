use crate::error::Error;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Operation {
    Create,
    Update,
    Delete,
}

impl TryFrom<String> for Operation {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "create" => Ok(Self::Create),
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            _ => Err(Error::UnknownOperation(value)),
        }
    }
}
impl TryFrom<&str> for Operation {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "create" => Ok(Self::Create),
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            _ => Err(Error::UnknownOperation(value.into())),
        }
    }
}
impl TryFrom<&String> for Operation {
    type Error = Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        match (*value).as_str() {
            "create" => Ok(Self::Create),
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            _ => Err(Error::UnknownOperation(value.clone())),
        }
    }
}

impl From<Operation> for String {
    fn from(op: Operation) -> Self {
        match op {
            Operation::Create => String::from("create"),
            Operation::Update => String::from("update"),
            Operation::Delete => String::from("delete"),
        }
    }
}
impl From<&Operation> for String {
    fn from(op: &Operation) -> Self {
        match op {
            Operation::Create => String::from("create"),
            Operation::Update => String::from("update"),
            Operation::Delete => String::from("delete"),
        }
    }
}

impl AsRef<str> for Operation {
    fn as_ref(&self) -> &str {
        match self {
            Operation::Create => "create",
            Operation::Update => "update",
            Operation::Delete => "delete",
        }
    }
}

impl PartialEq for Operation {
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}
