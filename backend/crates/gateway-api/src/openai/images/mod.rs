//! OpenAI/Codex Images JSON 透明 adapter。

mod http;

pub(crate) use http::{image_edits, image_generations};

pub(in crate::openai) use http::image_operation;
