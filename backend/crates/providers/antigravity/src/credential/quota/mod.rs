//! Antigravity 账号额度文档、刷新与调度投影入口

mod document;
mod service;

pub use document::Document as QuotaDocument;
pub(crate) use service::{QuotaService, QuotaTask};
