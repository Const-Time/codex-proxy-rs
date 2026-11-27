# v3.18.25

- 接入 Antigravity OAuth 账号和原生 Google GenAI 接口，支持模型发现、额度读取、分组模型变体、专用搜索路由和 `countTokens`。
- 增加 Antigravity Desktop 与 CLI 配置预览、保存和官方版本下载链接。授权使用部署时配置的 Google OAuth 客户端。
- 支持 `/v1/chat/completions` 和原生 Anthropic `/v1/messages`。配置为 Anthropic Messages 的官方 API 账号可通过 `/v1/messages/count_tokens` 计数。
- 修复流式请求的开场、终止和错误交付，保留已确认的用量。模型路由插件改写模型后，专用搜索仍会核验原始模型、选中模型和搜索目标的账号权限。
