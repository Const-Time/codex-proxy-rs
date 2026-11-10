# 上游检查：2026-11-10

检查范围：zyycn/codex-proxy-rs 最近 35 条主分支提交及最近更新的 35 个 PR，并补查后续新增的两条提交；以 GitHub API 查询时主分支 `13edec0b07d941070d0bbfc124ae349e163969de` 为准。以下是建议，不代表已移植。本轮发布仅包含生产请求间隔与原生续接容量错误修复。

| 优先级 | 上游 | 本地对比与建议 |
| --- | --- | --- |
| 高 | [#662 OpenSSL 安全更新](https://github.com/zyycn/codex-proxy-rs/pull/662) | 本地仍固定 openssl 0.10.75 / openssl-sys 0.9.111。上游 #662 升至 0.10.78 / 0.9.114，后续 #657 又升级到 0.10.80 / 0.9.116。建议审计后采用修复版本，按项目约束核验 TLS ClientHello；不能仅改版本号就认定画像无变化。 |
| 高 | [725de6e 替换被撤回的依赖版本](https://github.com/zyycn/codex-proxy-rs/commit/725de6ead6506651ff32db2e3bfb0a0798c3a7a3) | 本地同样锁定 crypto-bigint 0.7.6 / ctutils 0.4.5；上游更新至 0.7.11 / 0.4.8。建议随依赖维护迁移；撤回版本不等同于已确认本地存在可利用漏洞。 |
| 高 | [#667 按账号聚合模型目录](https://github.com/zyycn/codex-proxy-rs/pull/667) | 本地仍有按套餐共享目录逻辑。上游改为账号范围内目录并集、按账号实际模型资格选号，可避免同套餐不同模型权限造成漏模型或错选。需要保留我们的分组权限及模型白名单，并验证多账号请求量；上游说明未进行大池压测。 |
| 高 | [#657 错误来源与响应诊断](https://github.com/zyycn/codex-proxy-rs/pull/657) | 能将本地调度拒绝、上游响应、连接异常区分展示，与本次生产误读错误的场景直接相关。移植 Admin 分类、Store 只读证据与本地 Vue 界面，不直接替换上游不同 UI 组件。 |
| 中 | [c6350fd 未完成但明确失败的展示](https://github.com/zyycn/codex-proxy-rs/commit/c6350fd234a2501dc5475b7865f40843f7f6100f) | 建议随 #657 移植，只改变展示，不篡改原始 outcome、统计和计费。 |
| 中 | [6285332 Codex 配置启用模型发现](https://github.com/zyycn/codex-proxy-rs/commit/628533216a1754f2473c2bf998994fdb4b4aab47) | 本地配置模板缺少 api_key_model_discovery。建议与 #667 配套补充并验证生成配置，保留现有 WebSocket 和客户端认证选择。 |
| 中 | [13edec0 测试稳定性](https://github.com/zyycn/codex-proxy-rs/commit/13edec0b07d941070d0bbfc124ae349e163969de) | 本地仍有固定的跨平台原生 TLS 预期以及握手前暂停虚拟时钟的 WebSocket 测试。上游保留 Linux ClientHello 固定样本，其他平台比较系统原生 TLS，并在真实握手后推进虚拟时间、接受取消时的连接重置。建议适配测试，可减少跨平台误报与时序偶发失败，不改变线上传输逻辑。 |
| 按需 | [#625 分离排队时限及账号回退](https://github.com/zyycn/codex-proxy-rs/pull/625) | 本地没有完整对应排队体系。会影响 Core、设置、数据库和插件 SDK，需独立设计与迁移；不是把本次 50ms 间隔修复替换成大规模排队改造。 |
| 按需 | [#660 显式预算窗口与安全重试](https://github.com/zyycn/codex-proxy-rs/pull/660) | 新增插件读写预算窗口、版本化幂等重试与恢复约束。本地存在用户订阅、共享额度和账本差异，需先定义权威边界，避免与现有窗口重复计费或重置。 |
| 按需 | [dd65665 Messages 接入与 API 上游](https://github.com/zyycn/codex-proxy-rs/commit/dd65665128bc1066a14614804d7d86ecebf5c8d9) | 约 1.15 万行增删，涉及协议、鉴权、工具、流式响应、SDK 和计费。作为独立功能版本评估，不混入稳定性补丁。 |

已有对应能力，不重复迁移：#644 插件按代理 ID HTTP 请求与代理目录、#638 关闭时释放账号并发租约、#627 发布渠道索引，以及已对齐的页面滚动优化。

额外注意：上游 092789f 的 SQLx 更新包含其自有 Git 固定版本和 SQLite 安全支持；本地使用 SQLx 0.9.0、主要启用 PostgreSQL，不能仅凭提交标题断言本地受同一漏洞影响，应根据实际依赖树审计。#653 及后续账号预热调度与本地体检插件定时任务重叠，需要防止增加第二套主动请求任务。

## 后续适配：v3.18.9

按确认范围移植安全依赖、#667 账号目录、#657 错误来源、c6350fd 明确失败展示、6285332 客户端模型发现配置和 13edec0 传输测试稳定性。

- OpenSSL / openssl-sys 更新至 0.10.80 / 0.9.116，crypto-bigint / ctutils 更新至 0.7.11 / 0.4.8；保留 Linux 官方 ClientHello 固定样本。
- OpenAI 模型目录按账号、凭据版本及套餐隔离；Key 范围内合并，保留账号模型规则、原生模型元数据、32 条/32 MiB 缓存预算、单账号请求合并与失败短缓存。OAuth / API 各限并发 4，客户端查询共享 15 秒期限。
- 保留本地部分刷新语义：仅复用失败账号同一作用域的成功证据，不延长其 Redis TTL；健康账号独立更新，部分失败不确认 ETag 完成。账号替换不继承旧账号权限。
- 诊断复用本地错误记录与详情组件，仅详情读取已有原始错误及连接事实，不扩展用量列表热查询；区分代理、上游、连接和未知来源。展示失败不修改请求 outcome、统计和结算。
- 无数据库迁移或插件协议变更；不包含 #625、#660 或 Messages 大规模改造。
- 本地验证：OpenAI 835、Core 488、Admin 179、API 529、Store 310、插件 Runtime 398 项通过；Store/Runtime 原有 13 项手工或专用环境测试保持忽略。模块布局调整后架构 12 项重跑通过，workspace 严格 Clippy、Rust fmt、前端 88 项测试/lint/build、Compose 示例配置校验及 Edge 诊断卡片核验通过。
