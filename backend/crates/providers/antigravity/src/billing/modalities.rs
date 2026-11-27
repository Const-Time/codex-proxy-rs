use serde_json::{Map, Value};

/// Whether the request's actual modalities have supported reference pricing.
pub fn request_is_priceable(body: &Map<String, Value>) -> bool {
    let system = body
        .get("systemInstruction")
        .or_else(|| body.get("system_instruction"));
    let config = body
        .get("generationConfig")
        .or_else(|| body.get("generation_config"));
    let output_modes = config.and_then(|config| {
        config
            .get("responseModalities")
            .or_else(|| config.get("response_modalities"))
    });
    let output_formats = config.and_then(|config| {
        config
            .get("responseFormat")
            .or_else(|| config.get("response_format"))
    });
    array(body.get("contents")).all(content_is_priceable)
        && system.is_none_or(content_is_priceable)
        // GenerationConfig.Modality assigns 3 to AUDIO.
        && !array(output_modes).any(|mode| mode.as_u64() == Some(3) || audio_or_video(mode))
        && !array(output_formats).any(|format| {
            format.get("audio").is_some_and(Value::is_object)
                || format.get("video").is_some_and(Value::is_object)
        })
}

pub(crate) fn response_is_priceable(body: &Value) -> bool {
    let usage = body
        .get("usageMetadata")
        .or_else(|| body.get("usage_metadata"));
    array(body.get("candidates"))
        .filter_map(|candidate| candidate.get("content"))
        .all(content_is_priceable)
        && [
            ("promptTokensDetails", "prompt_tokens_details"),
            ("cacheTokensDetails", "cache_tokens_details"),
            ("candidatesTokensDetails", "candidates_tokens_details"),
            (
                "toolUsePromptTokensDetails",
                "tool_use_prompt_tokens_details",
            ),
        ]
        .into_iter()
        .all(|(json, proto)| {
            !array(usage.and_then(|usage| usage.get(json).or_else(|| usage.get(proto))))
                .filter_map(|detail| detail.get("modality"))
                // The usage Modality enum assigns 3 to VIDEO and 4 to AUDIO.
                .any(|mode| matches!(mode.as_u64(), Some(3 | 4)) || audio_or_video(mode))
        })
}

fn content_is_priceable(content: &Value) -> bool {
    array(content.get("parts")).all(|part| {
        media_is_priceable(part)
            && array(
                part.get("functionResponse")
                    .or_else(|| part.get("function_response"))
                    .and_then(|response| response.get("parts")),
            )
            .all(media_is_priceable)
    })
}

fn media_is_priceable(part: &Value) -> bool {
    ["inlineData", "inline_data", "fileData", "file_data"]
        .iter()
        .filter_map(|key| part.get(key))
        .filter_map(|media| media.get("mimeType").or_else(|| media.get("mime_type")))
        .filter_map(Value::as_str)
        .all(|mime| {
            mime.split_once('/').is_none_or(|(kind, _)| {
                !kind.eq_ignore_ascii_case("audio") && !kind.eq_ignore_ascii_case("video")
            })
        })
}

fn audio_or_video(value: &Value) -> bool {
    value.as_str().is_some_and(|mode| {
        mode.eq_ignore_ascii_case("AUDIO") || mode.eq_ignore_ascii_case("VIDEO")
    })
}

fn array(value: Option<&Value>) -> impl Iterator<Item = &Value> {
    value.and_then(Value::as_array).into_iter().flatten()
}
