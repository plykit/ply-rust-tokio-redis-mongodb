use serde::{Deserialize, Serialize};
use crate::error::PlyError;
use crate::Operation;

#[derive(Debug, Serialize,Deserialize)]
pub struct Msg {
    pub principal: String,
    pub op: Operation,
    pub id: String,
    // TODO Rename to type (this is entity type name)
    pub kind: String,
    pub bytes: Vec<u8>,
}

// TODO rename from/to msg to 'entity' and combine
pub trait ToMsg {
    fn kind() -> &'static str;
    fn id(&self) -> String;
    fn to_bytes(&self) -> Vec<u8>;
    fn into_msg(self, principal: String, op: Operation) -> Msg where Self: Sized {
        Msg{
            principal,
            op,
            id: self.id(),
            kind: String::from(Self::kind()),
            bytes: self.to_bytes()
        }
        
    }
}

pub trait FromMsg: Sized {
    fn kind() -> &'static str;
    // TDO make this try from
    fn from_bytes(msg: Vec<u8>) -> Result<Self, PlyError>;
}

// TODO explain why we do not use tryFrom, Into
// TODO or maybe we shouyld?  but ho wto constrain all this then?
