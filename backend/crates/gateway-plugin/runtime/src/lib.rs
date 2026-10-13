//! 上游插件包和 RPC 协议适配；本地管理生命周期通过 Admin 端口组合。
mod account_scheduling;
mod callbacks;
mod data;
mod engine;
mod management_validation;
pub mod middleware;
mod model_routing;
mod models;
mod package;
mod resources;
mod retry_policy;
mod rpc;
mod stream;
pub use engine::{HttpDispatcher, PluginEngine};
pub use package::{PackageError, PackageLimits, PreparedPackage, ValidatedPackage};
pub use rpc::{CallbackHandler, RpcError, RpcLimits, RpcReply, RpcSession};
pub use stream::RpcStream;

mod quota;

mod authentication;
