//! 与会话身份密钥作域隔离；数据库和备份中不出现代理认证信息或 state 原文。
use aws_lc_rs::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use gateway_admin::model::AdminError;

pub(super) struct StateCipher(LessSafeKey);
const AAD: &[u8] = b"codex-proxy-rs/turn-state/v1";

impl StateCipher {
    pub(super) fn new(secret: &[u8; 32]) -> Result<Self, AdminError> {
        Ok(Self(LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, secret).map_err(|_| error())?,
        )))
    }

    pub(super) fn seal(&self, mut bytes: Vec<u8>) -> Result<Vec<u8>, AdminError> {
        let mut nonce = [0; 12];
        getrandom::fill(&mut nonce).map_err(|_| error())?;
        self.0
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(AAD),
                &mut bytes,
            )
            .map_err(|_| error())?;
        let mut result = nonce.to_vec();
        result.extend(bytes);
        Ok(result)
    }

    pub(super) fn open(&self, mut bytes: Vec<u8>) -> Result<Vec<u8>, AdminError> {
        if bytes.len() < 28 || bytes.len() > 4 * 1024 * 1024 {
            return Err(error());
        }
        let nonce: [u8; 12] = bytes[..12].try_into().map_err(|_| error())?;
        self.0
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(AAD),
                &mut bytes[12..],
            )
            .map(|plain| plain.to_vec())
            .map_err(|_| error())
    }
}

fn error() -> AdminError {
    AdminError::unavailable("State 密钥或加密存储不可用，请保留 identity_hmac_secret")
}
