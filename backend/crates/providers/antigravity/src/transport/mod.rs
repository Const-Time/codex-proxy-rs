pub(crate) mod account;
mod client;
mod count;
mod failure;
mod response;

pub use client::{Catalog, CloudCodeClient, GenerateMode};
pub(crate) use client::{Model, unwrap_frame};
pub use count::decode_count_response;
pub(crate) use failure::{sent, unsent};
pub use response::{CloudCodeResponseContext, decode_cloud_code_response};

use gateway_core::account::{InvalidOutboundProxy, OutboundProxy};

pub(crate) fn http_client(
    proxy: Option<&OutboundProxy>,
) -> Result<reqwest::ClientBuilder, InvalidOutboundProxy> {
    let builder = reqwest::Client::builder().no_proxy();
    match proxy {
        Some(proxy) => {
            let proxy =
                reqwest::Proxy::all(proxy.expose_url()).map_err(|_| InvalidOutboundProxy)?;
            Ok(builder.proxy(proxy))
        }
        None => Ok(builder),
    }
}
