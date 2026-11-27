//! 内置参考价与外部来源映射共用一份明确的模型登记
// 2026-11-24 核验，USD / 百万 Token：
// https://aistudio.google.com/docs/pricing
// https://platform.claude.com/docs/en/about-claude/pricing
// https://cloud.google.com/model-garden/partner-models/pricing
// 仅登记公开基础模型及其思考档位；Tab、sonar、tiered 等内部型号不猜测对应关系

use gateway_core::metering::{ModelPriceOverride, TokenPriceOverride};
use std::{collections::BTreeMap, sync::LazyLock};

struct Family {
    models: &'static [&'static str],
    standard: [&'static str; 4],
    long: Option<[&'static str; 4]>,
    image: Option<[&'static str; 4]>,
}

const FAMILIES: &[Family] = &[
    Family {
        models: &[
            "claude-opus-5-5-low",
            "claude-opus-5-5-medium",
            "claude-opus-5-5-high",
        ],
        standard: ["4", "20", "0.2", "5"],
        long: None,
        image: None,
    },
    Family {
        models: &[
            "claude-sonnet-5-5-low",
            "claude-sonnet-5-5-medium",
            "claude-sonnet-5-5-high",
        ],
        standard: ["2", "10", "0.1", "2.5"],
        long: None,
        image: None,
    },
    Family {
        models: &["gemini-2.5-flash", "gemini-2.5-flash-thinking"],
        standard: ["0.3", "2.5", "0.03", "0.3"],
        long: None,
        image: None,
    },
    Family {
        models: &["gemini-2.5-flash-lite"],
        standard: ["0.1", "0.4", "0.01", "0.1"],
        long: None,
        image: None,
    },
    Family {
        models: &["gemini-2.5-pro"],
        standard: ["1.25", "10", "0.125", "1.25"],
        long: Some(["2.5", "15", "0.25", "2.5"]),
        image: None,
    },
    Family {
        models: &["gemini-3-flash"],
        standard: ["0.5", "3", "0.05", "0.5"],
        long: None,
        image: None,
    },
    Family {
        models: &["gemini-3.1-flash-lite"],
        standard: ["0.25", "1.5", "0.025", "0.25"],
        long: None,
        image: None,
    },
    Family {
        models: &["gemini-3.5-flash-lite"],
        standard: ["0.3", "2.5", "0.03", "0.3"],
        long: None,
        image: None,
    },
    Family {
        models: &[
            "gemini-3.8-flash-low",
            "gemini-3.8-flash-medium",
            "gemini-3.8-flash-high",
        ],
        standard: ["0.75", "3.75", "0.075", "0.75"],
        long: None,
        image: None,
    },
    Family {
        models: &[
            "gemini-4-argon-low",
            "gemini-4-argon-medium",
            "gemini-4-argon-high",
        ],
        standard: ["2", "10", "0.1", "2"],
        long: None,
        image: None,
    },
    Family {
        models: &[
            "gemini-4-carbon-low",
            "gemini-4-carbon-medium",
            "gemini-4-carbon-high",
        ],
        standard: ["2", "10", "0.1", "2"],
        long: None,
        image: None,
    },
    Family {
        models: &[
            "gemini-4-hydrogen-low",
            "gemini-4-hydrogen-medium",
            "gemini-4-hydrogen-high",
            "gemini-4-hydrogen-xhigh",
        ],
        standard: ["1.5", "7.5", "0.075", "1.5"],
        long: None,
        image: None,
    },
    Family {
        models: &[
            "gemini-4-neon-low",
            "gemini-4-neon-medium",
            "gemini-4-neon-high",
            "gemini-4-neon-xhigh",
        ],
        standard: ["3", "15", "0.15", "3"],
        long: None,
        image: None,
    },
    Family {
        models: &["gemini-3.1-flash-image"],
        standard: ["0.5", "3", "0.5", "0.5"],
        long: None,
        image: Some(["0.5", "60", "0.5", "0.5"]),
    },
    Family {
        models: &["gpt-oss-120b-medium"],
        standard: ["0.09", "0.36", "0.09", "0.09"],
        long: None,
        image: None,
    },
];

pub(crate) fn defaults() -> &'static BTreeMap<String, ModelPriceOverride> {
    static CATALOG: LazyLock<BTreeMap<String, ModelPriceOverride>> = LazyLock::new(|| {
        let mut result = BTreeMap::new();
        for family in FAMILIES {
            let mut bands = BTreeMap::from([("standard".to_owned(), rates(family.standard))]);
            if let Some(long) = family.long {
                bands.insert("long_standard".to_owned(), rates(long));
            }
            if let Some(image) = family.image {
                bands.insert("image".to_owned(), rates(image));
            }
            let price = ModelPriceOverride {
                multiplier_bps: 10_000,
                multiplier_scope: Default::default(),
                bands,
            };
            for model in family.models {
                result.insert((*model).to_owned(), price.clone());
            }
        }
        result
    });
    &CATALOG
}

fn rates([input, output, cache_read, cache_write]: [&str; 4]) -> TokenPriceOverride {
    let price = |value: &str| {
        value
            .to_owned()
            .try_into()
            .expect("valid builtin token price")
    };
    TokenPriceOverride {
        input: price(input),
        output: price(output),
        cache_read: price(cache_read),
        cache_write: price(cache_write),
    }
}
