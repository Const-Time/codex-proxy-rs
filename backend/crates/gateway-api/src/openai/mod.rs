//! OpenAI 客户端协议 adapter。

pub mod auth;
mod chat_completions;
mod endpoint;
pub mod error;
mod execution;
mod genai;
pub mod images;
pub(crate) mod ingress;
mod messages;
pub mod middleware;
pub mod models;
pub mod responses;
pub mod router;
pub mod search;
pub(crate) mod service;

mod revocation;
