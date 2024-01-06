use redis::RedisError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("operation `{0}` is not known, valid operations: 'create', 'update', 'delete'")]
    UnknownOperation(String),

    #[error("json parse error: `{0}`")]
    ParseError(#[from] serde_json::Error),

    #[error("redis error: `{0}`")]
    RedisError(#[from] RedisError),

    #[error("Jobs error: `{0}`")]
    JobsError(#[from] ply_jobs::Error),

    //#[error("UTF8 error: `{0}`")]
    //Utf8Error(#[from] FromUtf8Error),
    #[error("TODO(`{0}`)")]
    TODO(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub fn ok() -> Result<()> {
    Ok(())
}
