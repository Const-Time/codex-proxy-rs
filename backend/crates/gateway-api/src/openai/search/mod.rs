//! Codex standalone search HTTP adapter。

mod http;

pub(crate) use http::standalone_search;

pub(in crate::openai) use http::search_operation;
