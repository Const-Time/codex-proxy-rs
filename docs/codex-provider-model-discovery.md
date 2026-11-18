# 自定义 Provider 的模型目录排查

2026-11-18，在 Windows Codex `0.169.0-alpha.11.1` 实测：代理 `/v1/models`、`/v1/models?client_version=0.169.0` 均包含 `gpt-6.2-sol`、`gpt-6.2-astra`。Codex 专用目录同时包含 `additional_speed_tiers: ["fast"]` 和 `service_tiers` 的 `priority` 档位。

同一配置启动的 `codex app-server`，`model/list` 返回 9 个旧模型；改变 Provider 名称或 `requires_openai_auth` 均未恢复新模型。用 `model_catalog_json` 指定完整目录快照后，返回 10 个可见模型，包含上述两个模型及 Fast 能力。这说明该客户端的自定义 Provider 目录发现路径与直接查询代理存在差异。

临时办法是在 `config.toml` 顶层配置 `model_catalog_json` 为从当前代理取得的 Codex 目录 JSON 文件绝对路径，再重启客户端。它是静态快照，需要在模型变化或更换 Provider 后更新或移除，不应把价格表当作模型能力目录。不能仅凭模型出现在目录中保证所有账号都有调用权限。

本机已验证 `fast_mode = true`，且配置的 `service_tier = "priority"`。菜单不显示 Fast 不等于请求未指定 Fast；桌面按钮受客户端状态影响，服务端返回完整能力不能保证按钮出现。更改登录模式未解决模型发现问题，不应为此要求用户重新登录或改动认证。

本机临时配置已指向 `C:/Users/work/.codex/cprs-model-catalog.json`，原配置备份为 `C:/Users/work/.codex/config.before-model-catalog-20261118.toml`。不带命令行覆盖的 app-server 已列出两个 6.2 模型及 `priority` 档位；桌面菜单仍需用户重启后确认。未修改安装包或认证配置。
