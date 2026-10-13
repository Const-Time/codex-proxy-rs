use serde_json::json;

pub(super) fn connection(value: &str) -> serde_json::Value {
    let mut url = url::Url::parse(value).unwrap();
    let password = url.password().unwrap_or_default().to_owned();
    url.set_password(None).unwrap();
    json!({"url":url.as_str(),"password":password})
}
