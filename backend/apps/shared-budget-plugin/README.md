# 共享额度管理插件

管理员可查询指定用户与分组的共享用量、调整分组基础限额，以及显式重置该用户在该分组的每日和每周用量。用户与分组 ID 可从现有管理页面获取；同一用户在同一分组的全部 Key 共用这些窗口。

## 安装

使用 3.13.0 或更新的兼容网关。从包含此插件的网关 Release 附件选择与网关运行平台一致的 `cpr.shared-budget-manager-0.2.0-<target>.tar.gz`。在「插件」上传、创建实例，配置保持 `{}`，确认信任后启用，打开「共享额度管理」。不需要选择推理 Key。

插件仅使用公开 SDK 的管理页面、私有状态和本地共享额度接口。它以 `trustedProcess` 运行，具有网关进程的系统权限；只安装可信制品。源码仅依赖 SDK，不链接 Core、Admin 或 Store。

## 使用与恢复

- 查询由点击触发，不推进过期的额度窗口；零限额表示不限额。
- 修改基础限额影响整个分组，必须勾选确认。保存使用查询时的配置版本；冲突或结果未知时重新查询后再决定，页面不自动重发。
- 重置先保存目标及 UUIDv4 操作编号，再由管理员单独确认执行。页面刷新、插件重启或回复丢失后读取最近操作，使用原编号重试。宿主账本保证相同编号不会重复重置。
- 一个实例保存一项最近操作，管理员共同查看。存在未确认操作时不能覆盖；归档需要明确确认，且不取消、撤销或证明原重置未生效。先记录编号并核对结果，再创建新操作。
- 停用保留草稿，卸载会删除实例私有状态。结果未确认时不要通过卸载、重建实例或更换编号重试。

插件没有自己的用户权限、订阅或账本。有效限额、审计、授权、配置 CAS 与重置幂等性由原宿主服务负责；重置不消费 OpenAI 上游重置卡，也不改变订阅周期。

## 从源码打包

在仓库 `backend/` 下使用与宿主一致的原生平台：

```bash
cargo build --release --locked -p cpr-shared-budget-plugin -p codex-proxy-plugin-cli
target="$(rustc -vV | sed -n 's/^host: //p')"
target/release/cpr-plugin package \
  --manifest apps/shared-budget-plugin/plugin.json \
  --binary target/release/cpr-shared-budget-plugin \
  --target "$target" --output-dir ../dist/plugins --resource-map web=web
```

资源映射相对于 manifest 所在目录。设置了 `CARGO_TARGET_DIR` 时请相应替换二进制路径。

## 验证

`cargo test --locked -p cpr-shared-budget-plugin --test main` 启动实际插件进程，验证宿主委托、草稿持久化、丢失回执后原编号重试及归档边界。

从仓库根目录执行 `bash .github/scripts/check-shared-budget-plugin`，使用 `CPR_TEST_DATABASE_URL` 和 `CPR_TEST_REDIS_URL` 指向专用测试数据库。该检查构建、打包并通过真实管理员 API 安装插件，验证原生限额 CAS、进程恢复、数据库重置、重复确认不会清除后续用量，以及普通用户和停用实例无法调用。
