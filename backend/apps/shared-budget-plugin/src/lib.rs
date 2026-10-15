//! Optional management workflow; all authorization and budget writes belong to the host.
pub mod app;
mod reset;

pub const PLUGIN_ID: &str = "cpr.shared-budget-manager";
pub const MANIFEST: &[u8] = include_bytes!("../plugin.json");
