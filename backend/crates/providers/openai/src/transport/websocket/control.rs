//! WebSocket 有界消息通道与原连接文本透传；空闲读取与响应正文消费互斥

use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::{self, Message};

use super::CodexWebSocketExchangeError;

pub(super) enum PumpCommand {
    Send {
        message: Message,
        ack: oneshot::Sender<Result<(), CodexWebSocketExchangeError>>,
    },
}

pub(super) async fn send_message(
    commands: &mpsc::Sender<PumpCommand>,
    message: Message,
) -> Result<(), CodexWebSocketExchangeError> {
    let (ack, rx_ack) = oneshot::channel();
    commands
        .send(PumpCommand::Send { message, ack })
        .await
        .map_err(|_| CodexWebSocketExchangeError::from(tungstenite::Error::ConnectionClosed))?;
    // 回执丢失不能证明未发送，只有 pump 明确拒绝的命令才允许按 NotSent 恢复
    rx_ack
        .await
        .unwrap_or_else(|_| Err(tungstenite::Error::ConnectionClosed.into()))
}
