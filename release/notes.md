# v3.18.3

- 支持 Codex Responses WebSocket steering：生成途中追加输入时使用当前上游连接，转发 accepted、pending、failed，并持续接收自动续接响应。
- 等待工具结果时保留原连接和上游已接受的输入；断连不自动重放不确定的 steering，控制消息不会误作用于后续响应。
- 多段响应独立计价后累加，整次执行统一结算；兼容请求和执行插件中间件，避免提前截断自动续接或重复计费。
- 修复 response.steer 和空闲时 response.interrupt 被误报为 type 参数无效；控制失败返回明确原因且不终止当前生成。
- 本地已通过 API、核心及 OpenAI Provider 共 1,839 项测试，以及格式和严格 Clippy 检查；发布流程继续运行全工作区测试和平台构建门禁。
