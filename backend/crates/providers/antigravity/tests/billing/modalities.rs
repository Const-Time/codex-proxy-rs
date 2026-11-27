use gateway_protocol::chat_completions::genai;
use serde_json::json;

use provider_antigravity::request_is_priceable;

#[test]
fn reads_media_fields_without_descending_into_business_json() {
    let media =
        json!({"fileData": {"mimeType": "Video/mp4", "fileUri": "https://example.invalid/video"}});
    let display_metadata = json!({"modality": "AUDIO", "mime_type": "audio/wav", "parts": [media]});
    let mut request = json!({
        "contents": [{
            "parts": [
                {"functionCall": {"name": "tag", "args": display_metadata}},
                {"functionResponse": {"name": "tag", "response": display_metadata}},
                {"inlineData": {"mimeType": "image/png", "data": "AA=="}}
            ]
        }],
        "systemInstruction": {"parts": [{"text": "system"}]},
        "labels": display_metadata,
    });
    assert!(request_is_priceable(request.as_object().unwrap()));
    request["contents"][0]["parts"][1]["functionResponse"]["parts"] = json!([media]);
    assert!(!request_is_priceable(request.as_object().unwrap()));
    request["contents"][0]["parts"][1]["functionResponse"]
        .as_object_mut()
        .unwrap()
        .remove("parts");
    request["systemInstruction"]["parts"] =
        json!([{"inline_data": {"mime_type": "audio/wav", "data": "AA=="}}]);
    assert!(!request_is_priceable(request.as_object().unwrap()));
}

#[test]
fn chat_audio_is_detected_at_the_converted_media_path() {
    let mut chat = json!({
        "model": "alias",
        "metadata": {"mimeType": "video/mp4", "modality": "AUDIO"},
        "messages": [{"role": "user", "content": "hello"}],
    });
    let converted = genai::convert_request(chat.as_object().unwrap()).unwrap();
    assert!(request_is_priceable(&converted));
    chat["messages"][0]["content"] = json!([{
        "type": "input_audio", "input_audio": {"format": "wav", "data": "AA=="}
    }]);
    assert!(!chat.to_string().contains("audio/wav"));
    let converted = genai::convert_request(chat.as_object().unwrap()).unwrap();
    assert_eq!(
        converted["contents"][0]["parts"][0]["inlineData"]["mimeType"],
        "audio/wav"
    );
    assert!(!request_is_priceable(&converted));
}
