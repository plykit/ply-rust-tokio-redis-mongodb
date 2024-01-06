use crate::error::Result;
use crate::Operation;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Msg {
    pub principal: String,
    pub op: Operation,
    pub id: String,
    // TODO Rename to type (this is entity type name)
    pub kind: String,
    pub bytes: Vec<u8>,
}

// TODO rename from/to msg to 'entity' and combine
pub trait ToMsg: Sized {
    fn kind() -> &'static str;
    fn id(&self) -> String;
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(msg: Vec<u8>) -> Result<Self>;
    fn into_msg(self, principal: String, op: Operation) -> Msg
    where
        Self: Sized,
    {
        Msg {
            principal,
            op,
            id: self.id(),
            kind: String::from(Self::kind()),
            bytes: self.to_bytes(),
        }
    }
}
