use crate::context::RpcContext;
use crate::v05::method::call::{CallInput, CallOutput};

#[derive(Debug)]
pub enum CallError {
    Internal(anyhow::Error),
    BlockNotFound,
    ContractNotFound,
    ContractError,
    Custom(anyhow::Error),
}
impl From<anyhow::Error> for CallError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e)
    }
}
impl From<CallError> for crate::error::ApplicationError {
    fn from(x: CallError) -> Self {
        match x {
            CallError::BlockNotFound => Self::BlockNotFound,
            CallError::ContractNotFound => Self::ContractNotFound,
            CallError::ContractError => Self::ContractError,
            CallError::Internal(internal) => Self::Internal(internal, ()),
            CallError::Custom(error) => Self::Custom(error),
        }
    }
}

impl From<crate::v05::method::call::CallError> for CallError {
    fn from(value: crate::v05::method::call::CallError) -> Self {
        match value {
            crate::v05::method::call::CallError::Internal(e) => Self::Internal(e),
            crate::v05::method::call::CallError::BlockNotFound => Self::BlockNotFound,
            crate::v05::method::call::CallError::ContractNotFound => Self::ContractNotFound,
            crate::v05::method::call::CallError::ContractErrorV05 { revert_error } => {
                Self::Custom(anyhow::anyhow!("Transaction reverted: {}", revert_error))
            }
        }
    }
}

// The implementation is the same as for v05 -- the only difference is that we have to map
// ContractErrorV05 to an internal error.
pub async fn call(context: RpcContext, input: CallInput) -> Result<CallOutput, CallError> {
    crate::v05::method::call::call(context, input)
        .await
        .map_err(Into::into)
}
