# 上游插件体系移植方案

状态：方案，尚未实施。评估基线为本地 `d400d9a2`（v3.11.1），上游 `d83d3eb80396b454c8120612a9ffed03d57af72c`（2026-09-28）。

## 结论与范围

可以移植，但不能直接 cherry-pick 最新插件提交。上游插件体系已经贯穿 Core 执行生命周期、进程通信、制品管理、管理页面、受管上游适配器和模型回调；本地仍有独立的用户、账号分组、订阅账本及取消安全结算实现。

建议先实现“安装、启停、只读能力”的可运行版本，再接中间件，最后开放模型调用与上游适配器。首版兼容 SDK 清单与通信协议的已选子集，安装时明确拒绝尚未支持的能力；不能把缺失能力默认为成功。

这里的插件是此网关的服务端扩展，与 Codex 桌面插件不是同一套格式。上游使用本机可执行进程；进程分离不等于安全沙箱。首版应只允许管理员安装可信制品，不能宣传为可安全运行任意第三方代码。

## 已核对的上游组成

| 部分 | 上游位置 | 本地落点与处理 |
| --- | --- | --- |
| SDK、清单、消息、帧 | `backend/crates/gateway-plugin/sdk` | 保持独立 crate；默认不依赖宿主 Core / Store，保留协议版本校验 |
| 进程、RPC、回调、流与 generation | `backend/crates/gateway-plugin/runtime` | 新 runtime crate，具体组合仍留在 `apps/gateway`；通过 Core/Admin 端口接入 |
| 制品分发、进程生命周期 | `gateway-host/src/plugin_distribution` | Host 负责下载、校验、解包、启动、排空和退出；安装状态不另建文件权威 |
| 制品、实例、配置、资源 | `gateway-store/src/postgres/plugins` | 新增 0027 起的迁移；映射到本地用户与审计合同 |
| 管理 API 与业务用例 | `gateway-admin`、`gateway-api` 的 plugins 模块 | 复用本地管理员鉴权、MutationActor、中文错误和审计 |
| 管理页面与资源入口 | 上游 frontend 的 plugins 页面 | 接入现有布局与权限；路由和资源按实例限定 |
| 打包工具 | `backend/apps/plugin-cli` | 单独后续导入；首版可用测试制品走完整安装链 |

上游 SDK 标注为实验阶段，当前 manifestVersion / protocolVersion 为 1，middleware 可声明 v2。必须固定提交与支持矩阵，不以“最新版兼容”作为合同。

参考：[SDK](https://github.com/zyycn/codex-proxy-rs/tree/d83d3eb80396b454c8120612a9ffed03d57af72c/backend/crates/gateway-plugin/sdk)、[Runtime](https://github.com/zyycn/codex-proxy-rs/tree/d83d3eb80396b454c8120612a9ffed03d57af72c/backend/crates/gateway-plugin/runtime)、[插件文档](https://github.com/zyycn/codex-proxy-rs/blob/d83d3eb80396b454c8120612a9ffed03d57af72c/docs/plugins.md)。

## 必须保留的本地合同

- 已发布的 0001–0026 迁移逐字节冻结。上游 0018/0019 不能按原编号导入，更不能替换上游重组后的历史基线。
- 本地 User → Key → 账号分组授权保持唯一权威。外部 principal 不能直接变成本地管理员或无范围的 Key。
- 请求开始时冻结账号范围、模型政策、分组、倍率及价格；热更新仅影响新请求，不能让在途请求扩大权限。
- 插件的模型回调进入 Core 的正常准入、路由、租约、结算与观测路径。不能直接绕过账本调用 Provider。
- 子调用要有独立执行 ID、根/父关联和幂等费用键；限定深度、并发、总预算，并明确父子费用归集方式，禁止根请求与子请求双计费。
- 取消、断连、插件崩溃、热替换都必须收敛并发租约与账本。已提交或发送结果不明的流不得自动重放。
- credential、Cookie、账号身份、FedRAMP/路由头、continuation owner 仍由宿主/Provider 重建。插件不得通过 header 或父调用 ID 提升权限。
- 配置、制品和实例以 PostgreSQL 为权威；Redis 只做可过期协调。敏感数据沿用现有加密与备份恢复体系，不出现在普通日志、配置导出或审计详情中。

## 分阶段交付

### 1. 独立 SDK 与协议验证

导入 SDK 和协议测试，确定首版支持能力清单。建立合成插件 worker，验证握手、版本、注册声明、帧边界、父调用关联、超时、取消和关闭。对元数据、并发、队列及流信用设置资源边界；不要为兼容完整正文处理而假设内存无限。

验收：未知协议/能力不能安装或启用；错误帧和超限 worker 不拖死宿主；默认关闭时现有 workspace 回归不变。

### 2. 安装管理与只读能力

实现制品校验、解包路径保护、实例启停、配置校验、状态观测与 generation 发布。新增迁移保存制品及实例，复用本地审计、管理员权限和备份。先开放严格限定的目录/只读观测能力，不开放任意账号 secret、模型调用或动态认证。

验收：安装失败不留下半启用实例；重启可恢复；禁用与升级先排空旧 generation；撤权不会被缓存恢复。完成一个只读示例从上传到卸载的 UI/API 回归。

### 3. HTTP/流中间件

在 Core 设计单一 Prepared Execution 边界，兼容现有 `ExecutionSession` 的取消和结算责任，再接 single-use next、惰性流与能力声明。API 继续只负责协议适配，Provider 继续拥有传输与凭据；不可平行保留两套执行状态机。

验收：HTTP、SSE、WS 的首包、commit barrier、终态及断连测试全部覆盖；中间件不能修改宿主受管身份；热替换不影响在途流；处理控制帧不取消正在读取的正文 future。

### 4. 模型回调与受管上游适配器

先实现父/子调用授权交集和账本归集，再开放模型回调。随后适配 OpenAI/xAI 的受管上游端点、连接状态、重试提示和 native continuation。账号调度策略也只能在已授权候选集内选择。

验收：跨用户/Key/分组回调被拒绝；子请求失败或取消仍只结算一次；重试、跨日和价格变更不重算历史费用；原生续接和 response.interrupt 不能跨执行或跨账号。

### 5. 完善兼容能力

最后评估 CLI、管理资源、公开回调、外部认证与自动更新。每项分别写权限、资源和回滚合同。完整上游插件兼容需通过实际插件的合同测试，不能仅以 SDK 能编译为验收。

## 拆分与回滚

每阶段独立 PR，均可在插件总开关关闭时运行现有网关。数据库只追加迁移；关闭插件后保留实例/制品记录，不能用回滚 DDL 删除用户数据。升级通过准备新 generation、校验成功后切换、旧 generation 排空；失败保持当前 generation。

建议下一次实施只做到第 1–2 阶段。第 3–4 阶段涉及执行与计费合同，应各自单独评审。当前这批修复没有导入插件 runtime、安装接口或数据库表。
