//! Share compiled validators across configuration, state preparation and callbacks.
//! The cache retains only a bounded number; live generations own their validators.
use gateway_admin::model::AdminError;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, OnceLock},
};

type Entry = ([u8; 32], Arc<jsonschema::Validator>);
const CAPACITY: usize = 32;

#[derive(Default)]
struct Cache(Mutex<VecDeque<Entry>>);

impl Cache {
    fn get(&self, value: &Value) -> Result<Arc<jsonschema::Validator>, AdminError> {
        let bytes =
            serde_json::to_vec(value).map_err(|_| AdminError::invalid("插件 schema 无效"))?;
        let key: [u8; 32] = Sha256::digest(&bytes).into();
        let mut entries = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((_, validator)) = entries.iter().find(|(digest, _)| digest == &key) {
            return Ok(validator.clone());
        }
        // Serialize misses too: concurrent preparation must not compile the same
        // schema repeatedly, or create an unbounded burst of compiler work.
        let validator = Arc::new(
            jsonschema::options()
                .offline()
                .with_pattern_options(
                    jsonschema::PatternOptions::fancy_regex().backtrack_limit(20_000),
                )
                .build(value)
                .map_err(|_| AdminError::invalid("插件 schema 无效或引用外部资源"))?,
        );
        if entries.len() == CAPACITY {
            entries.pop_front();
        }
        entries.push_back((key, validator.clone()));
        Ok(validator)
    }
}

pub(crate) fn schema(value: &Value) -> Result<Arc<jsonschema::Validator>, AdminError> {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(Cache::default).get(value)
}
