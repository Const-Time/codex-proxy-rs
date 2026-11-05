//! Connection-local steering acknowledgements decide when an exchange may release its socket.
use super::reducer::WebSocketTerminalKind;
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Default)]
pub(super) struct SteeringState {
    unacknowledged: usize,
    accepted: BTreeSet<String>,
    awaiting_successor: bool,
    pub(super) terminal: Option<WebSocketTerminalKind>,
}

impl SteeringState {
    pub(super) fn sent(&mut self) {
        self.unacknowledged += 1;
    }

    pub(super) fn observe(&mut self, value: &Value) {
        let id = value.pointer("/steer/id").and_then(Value::as_str);
        match value.get("type").and_then(Value::as_str) {
            Some("response.steer.accepted") => {
                self.unacknowledged = self.unacknowledged.saturating_sub(1);
                if let Some(id) = id {
                    self.accepted.insert(id.to_owned());
                }
            }
            Some("response.steer.failed") => {
                if let Some(id) = id {
                    self.accepted.remove(id);
                } else {
                    self.unacknowledged = self.unacknowledged.saturating_sub(1);
                }
                if self.accepted.is_empty() && self.unacknowledged == 0 {
                    self.awaiting_successor = false;
                }
            }
            Some("response.steer.pending") => {
                if let Some(id) = id {
                    self.accepted.remove(id);
                }
                self.awaiting_successor = false;
            }
            Some("response.created") if self.terminal.is_some() => {
                self.accepted.clear();
                self.awaiting_successor = false;
                self.terminal = None;
            }
            Some("response.incomplete")
                if value
                    .pointer("/response/incomplete_details/reason")
                    .and_then(Value::as_str)
                    == Some("steered") =>
            {
                self.awaiting_successor = true;
            }
            _ => {}
        }
    }

    pub(super) fn finished(&self) -> Option<WebSocketTerminalKind> {
        let terminal = self.terminal?;
        if terminal == WebSocketTerminalKind::Failed
            || (self.unacknowledged == 0 && self.accepted.is_empty() && !self.awaiting_successor)
        {
            Some(terminal)
        } else {
            None
        }
    }
}
