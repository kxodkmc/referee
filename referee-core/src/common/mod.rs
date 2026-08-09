//! 公共数据类型：错误码与信封

pub mod envelope;
pub mod error;

pub use envelope::Envelope;
pub use error::{KernelError, KernelResult};
