//! 上游插件包和 RPC 协议适配；本地管理生命周期通过 Admin 端口组合。
mod account_scheduling;
mod callbacks;
mod catalog;
mod command_line;
mod data;
mod engine;
mod management_validation;
pub mod middleware;
mod model_routing;
mod models;
mod observation;
mod outbound;
mod package;
mod resources;
mod retry_policy;
mod rpc;
mod services;
mod stream;
pub mod upstream_adapter;
pub use engine::{HttpDispatcher, PluginEngine};
pub use package::{PackageError, PackageLimits, PreparedPackage, ValidatedPackage};
pub use rpc::{CallbackHandler, RpcError, RpcLimits, RpcReply, RpcSession};
pub use stream::RpcStream;

mod quota;

mod authentication;
