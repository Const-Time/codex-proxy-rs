//! 上游插件包和 RPC 协议适配；本地管理生命周期通过 Admin 端口组合。
mod callbacks;
mod data;
mod engine;
mod management_validation;
mod package;
mod rpc;
mod stream;
pub use engine::PluginEngine;
pub use package::{PackageError, PackageLimits, PreparedPackage, ValidatedPackage};
pub use rpc::{CallbackHandler, RpcError, RpcLimits, RpcReply, RpcSession};
pub use stream::RpcStream;
