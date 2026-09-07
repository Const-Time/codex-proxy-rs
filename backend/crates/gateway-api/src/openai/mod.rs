//! OpenAI 客户端协议 adapter。

pub mod auth;
mod endpoint;
pub mod error;
pub mod images;
pub mod models;
pub mod responses;
pub mod router;
pub mod search;
pub(crate) mod service;
