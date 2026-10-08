# 推理便笺

一个只依赖上游 SDK 的 management v1 插件：选择本人 Key 后，填写模型 ID 和问题，显示 Responses SSE 文本，支持取消。问题与回答仅在页面保存；页面刷新或更换 Key 会清空。模型 ID 必须是所选 Key 可用的精确 ID。

从仓库根目录在 Linux x86_64 构建并打包：

```bash
cargo +1.97 build --manifest-path backend/Cargo.toml -p gateway-plugin-sdk --example prompt-pad --features io --locked
cargo +1.97 run --manifest-path backend/Cargo.toml -p codex-proxy-plugin-cli --locked -- package \
  --manifest backend/crates/gateway-plugin/sdk/examples/prompt-pad/plugin.json \
  --binary backend/target/debug/examples/prompt-pad \
  --target x86_64-unknown-linux-gnu \
  --resource-map web=web \
  --output-dir dist/plugins
```

设置了 `CARGO_TARGET_DIR` 时，将二进制路径改为该目录下的 `debug/examples/prompt-pad`。上传生成的 `.tar.gz`，创建可信实例并启用，打开「推理便笺」，在宿主页面查询和选择本人 Key，再发送问题。使用正式 Key 会按现有额度和倍率计费。

推理调用使用上游页面桥 `models.responses`；本地宿主额外提供 `models.clientKeyId` 传递用户在父页面显式选择的 Key ID。其他宿主若没有这个便利字段，需要按其授权界面取得 Key ID；不应让用户把 Key 明文交给插件。

示例未使用原生进程的 `host.models.*`，也不要求中间件、资源创建或额度写入权限。当前支持边界见[插件文档](../../../../../../docs/plugins.md)。
