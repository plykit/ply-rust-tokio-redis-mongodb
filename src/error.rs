use thiserror::Error;
//use anyhow;

#[derive(Error, Debug)]
pub enum PlyError {
    #[error("operation `{0}` is not known, valid operations: 'create', 'update', 'delete'")]
    UnknownOperation(String),
    //#[error(transparent)]
    //AnyError(#[from] anyhow::Error),
    #[error("todo error")]
    Todo,
}

pub type PlyResult = Result<(), PlyError>;

pub fn ok() -> PlyResult {
    Ok(())
}
