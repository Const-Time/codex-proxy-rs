<!-- prettier-ignore -->
<div align="center">

<img src="frontend/public/favicon.svg" alt="Codex Proxy RS" width="80" height="80" />

# Codex Proxy RS

集中管理 OpenAI、xAI 账号，为 Codex 和兼容客户端提供统一的访问地址与密钥。

[![CI](https://github.com/zyycn/codex-proxy-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/zyycn/codex-proxy-rs/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/zyycn/codex-proxy-rs?display_name=tag&sort=semver&style=flat-square)](https://github.com/zyycn/codex-proxy-rs/releases)
[![GHCR](https://img.shields.io/badge/GHCR-codex--proxy--rs-2496ED?logo=docker&logoColor=white&style=flat-square)](https://github.com/zyycn/codex-proxy-rs/pkgs/container/codex-proxy-rs)
[![MIT](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square)](https://opensource.org/license/mit)

[快速开始](#快速开始) · [客户端接入](#客户端接入) · [常见问题](#常见问题) · [部署说明](deploy/README.md)

</div>

## 功能

- 在网页中添加账号，查看额度、请求记录和费用，刷新或重新授权过期账号。
- 用账号分组控制不同密钥可使用的账号，设置并发和请求频率。
- 接入 Codex CLI、桌面端和支持 Responses API 的客户端。
- 支持流式对话、WebSocket、图片生成与编辑、Codex 独立搜索。
- 支持 S3/R2 数据库备份、同大版本在线更新和回滚。

支持 OpenAI、xAI 账号，但图片与独立搜索目前仅通过 OpenAI 提供。模型和功能是否可用取决于上游账号权限。
**不支持 `/v1/chat/completions`**；仅支持该接口的客户端不能直接接入。

## 快速开始

以下命令适用于 Linux，需要 Git、Docker Engine 和 Docker Compose Plugin。已有部署请先看
[升级说明](deploy/README.md#镜像升级与源码构建)，不要覆盖原配置。

### 1. 下载并配置

```bash
git clone https://github.com/zyycn/codex-proxy-rs.git
cd codex-proxy-rs

mkdir -p .runtime/data .runtime/logs
install -d -m 0750 .runtime/postgres .runtime/redis
cp deploy/config.example.yaml deploy/config.yaml
sudo chown "$(id -u):10001" deploy/config.yaml
chmod 0640 deploy/config.yaml
sudo chown -R "$(id -u):10001" .runtime/data .runtime/logs
chmod 0770 .runtime/data .runtime/logs
```

分别生成数据库和 Redis 密码：

```bash
openssl rand -hex 24
openssl rand -hex 24
```

编辑 `deploy/config.yaml`，填好以下三项：

| 配置项 | 填写内容 |
| --- | --- |
| `store.database.password` | 第一个生成的 48 位十六进制密码 |
| `store.redis.password` | 第二个生成的 48 位十六进制密码 |
| `admin.default_password` | 管理员初始密码，至少 12 位，不能包含 `$` |

不需要创建 `.env`。管理员初始密码只在首次创建账号时使用。

### 2. 启动服务

```bash
docker compose -f deploy/compose.yaml config --quiet
docker compose -f deploy/compose.yaml pull
docker compose -f deploy/compose.yaml up -d --no-build
curl -i http://127.0.0.1:8080/healthz
```

健康检查返回 `204 No Content` 后，打开 `http://127.0.0.1:8080`，
使用 `admin@cpr.local` 和刚设置的管理员密码登录。

默认地址只能在服务器本机访问。从其他设备使用时，需要配置
[HTTPS 反向代理](deploy/README.md#公网访问)。

### 3. 添加账号和密钥

1. 在「账号」中添加 OpenAI 或 xAI 账号。OpenAI 支持 OAuth 授权、AT/RT 和账号 JSON；xAI 支持 OAuth 授权和账号 JSON。
2. 按需创建账号分组，并把账号加入分组。
3. 创建客户端密钥，选择可用分组。**不选分组表示可使用全部账号**。
4. 点击该密钥的「使用密钥」，复制客户端配置。

只导入 AT 的账号不能自动续期。xAI 暂不支持用 API Key 导入上游账号。

## 客户端接入

### Codex CLI 和桌面端

在「使用密钥」中按系统复制 `config.toml` 和 `auth.json`，也可以通过 CCSwitch 导入。
已有文件先备份，再合并配置并重启 Codex。客户端使用代理密钥，OpenAI 账号登录状态由服务端管理。

新配置已启用原生生图。可以直接提出需求，例如：

> 帮我做一个咖啡店首页，生成一张咖啡主题配图并用到页面中。

是否调用生图由 Codex 根据任务决定，也可以明确要求生成图片。需要支持生图的客户端、模型和 OpenAI 账号；
普通对话可用不代表账号一定有生图额度。

完整配置和旧版配置迁移见 [客户端配置](deploy/README.md#客户端配置)。

### 其他客户端

选择支持 **Responses API** 的客户端，填写：

| 配置 | 值 |
| --- | --- |
| Base URL | `http://127.0.0.1:8080/v1`，远程使用时换成服务器的 HTTPS 地址 |
| API Key | 管理端创建的 `sk_...` 密钥 |

可先检查密钥是否能读取模型列表：

```bash
curl http://127.0.0.1:8080/v1/models \
  -H 'Authorization: Bearer <client-api-key>'
```

接口列表与请求示例见 [接口文档](docs/api.md)。

## 常见问题

### 仍然要求登录，或无法生成图片

重新从管理端复制配置，确认修改的是客户端实际读取的用户配置目录，然后完全退出并重启客户端。
保留仅含代理密钥的 `auth.json` 不会使新配置失效。详细检查步骤见
[登录与生图排查](deploy/README.md#登录与生图排查)。

### 没有可用账号或模型

检查账号是否启用、凭据是否过期、额度是否耗尽，以及密钥绑定的分组是否包含可用账号。
「恢复账号」只清除本地错误状态，不能恢复上游权限或增加额度。

### 如何更新或查看日志

同一大版本内拉取发布镜像并重建应用容器：

```bash
docker compose -f deploy/compose.yaml pull codex-proxy-rs
docker compose -f deploy/compose.yaml up -d --no-build codex-proxy-rs
docker compose -f deploy/compose.yaml logs --tail=100 -f codex-proxy-rs
```

> [!WARNING]
> 升级前先备份。跨大版本需使用新的数据目录重新部署，不支持直接沿用旧库在线升级。
> `.runtime/` 包含数据库和运行数据，请勿删除；账号文件、密钥和日志也不要公开分享。

## 文档

- [客户端接入与生图](deploy/README.md#客户端配置)
- [部署、备份与恢复](deploy/README.md)
- [API 参考](docs/api.md)
- [系统架构](docs/architecture.md)
- [管理端主题](docs/theme.md)
- [数据库迁移](backend/migrations/README.md)
