# 账号导出

在「账号管理」选中账号后，点击「导出选中」，可以选择：

- **Codex 认证文件（默认）**：仅支持具备完整 OAuth 凭据的 OpenAI 账号。选中一个账号时下载 `auth.json`；多个账号时下载 ZIP，每个独立目录中包含一份 `auth.json`，目录名含序号和账号标识。解压后选择所需账号的文件使用或重新导入，ZIP 本身不是认证文件。
- **系统备份**：沿用原有 CPR JSON 导出，支持 OAuth、API Key、访问令牌及其他平台账号。

Codex 认证文件包含 `auth_mode: "chatgpt"`、`OPENAI_API_KEY: null` 以及 `tokens` 下的 `id_token`、`access_token`、`refresh_token`、`account_id`。仅使用当前保存的凭据；导出不会刷新 token，也不会把账号设置更新时间冒充 `last_refresh`。凭据缺失、混选其他平台、API Key 或访问令牌账号时会提示使用系统备份，整批导出中止，不静默跳过账号。

转换和 ZIP 打包在浏览器本地完成，仍通过现有管理员导出接口取得数据，沿用权限校验、导出数量上限、审计和 `no-store` 响应。打包代码按需加载，不在普通账号列表加载时初始化。认证文件不包含代理、模型策略、分组或调度设置，迁移这些设置时应选择系统备份。

[OpenAI 官方认证文档](https://learn.chatgpt.com/docs/auth#login-caching)介绍了 Codex 的 `auth.json` 文件存储。文件包含真实登录凭据，应保存在受控位置。有效性仍取决于原凭据状态；本次使用虚构凭据在隔离目录验证 Codex CLI 的格式识别，并验证本系统重新导入，不发送真实模型请求。
