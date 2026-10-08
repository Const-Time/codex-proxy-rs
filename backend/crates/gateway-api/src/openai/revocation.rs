//! Keep consuming revocation even while the downstream stops pulling the response body.
use axum::{body::Body, response::Response};
use futures::{StreamExt as _, stream};
use gateway_core::{lifecycle::CancellationToken, runtime::ClientAccessGuard};
use std::io;
struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
pub(crate) fn protect(response: Response, access: ClientAccessGuard) -> Response {
    let (parts, body) = response.into_parts();
    let stop = CancellationToken::new();
    let stopped = stop.clone();
    let revoked = access.cancellation();
    let receiver_revoked = revoked.clone();
    let delivery_access = access.clone();
    let (send, receive) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        let _access = access;
        let mut chunks = body.into_data_stream();
        loop {
            let chunk = tokio::select! {biased;
                _=stopped.cancelled()=>break,
                _=revoked.cancelled()=>break,
                chunk=chunks.next()=>match chunk {Some(v)=>v.map_err(io::Error::other),None=>break},
            };
            tokio::select! {biased;
                _=stopped.cancelled()=>break,
                _=revoked.cancelled()=>break,
                result=send.send(chunk)=>if result.is_err(){break;},
            }
        }
    });
    Response::from_parts(
        parts,
        Body::from_stream(stream::unfold(
            Some((
                receive,
                CancelOnDrop(stop),
                receiver_revoked,
                delivery_access,
            )),
            |state| async move {
                let (mut receive, stop, revoked, access) = state?;
                tokio::select! {biased;
                    _=revoked.cancelled()=>Some((Err(io::Error::other("client authorization revoked")),None)),
                    chunk=receive.recv()=>chunk.map(|v|(v,Some((receive,stop,revoked,access)))),
                }
            },
        )),
    )
}
