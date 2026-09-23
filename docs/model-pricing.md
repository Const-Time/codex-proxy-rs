# 模型价格与新模型计费

## 价格不是模型目录的一部分

OpenAI 的 `GET /v1/models` 返回模型标识等目录信息，不返回输入、输出、缓存、
Flex / Priority 或长上下文单价。刷新模型目录不等于刷新价格。

此前 OpenAI 的价格只来自 Provider 内置表，因此新模型即使能正常请求，也可能没有费用。
本次保留内置价目，同时增加 PostgreSQL 持久化的同步价格和人工覆盖。

官方参考：

- https://developers.openai.com/api/docs/models
- https://developers.openai.com/api/docs/pricing
- https://developers.openai.com/api/docs/pricing.md

2026-09-22 核验并补入的标准短上下文价格，单位 USD / 百万 Token：

| 模型 | 输入 | 缓存读取 | 缓存写入 | 输出 |
| --- | ---: | ---: | ---: | ---: |
| gpt-6-sol | 2 | 0.2 | 2.5 | 10 |
| gpt-6-luna | 0.1 | 0.01 | 0.125 | 0.5 |

同时提供官方 Flex、Priority/Fast 和 >272,000 输入 Token 的长上下文档位；
不是简单地给所有请求乘同一系数。实际请求继续以原有的响应 service tier 口径计费。
响应中展示的模型名称不替换实际发送的上游模型计价键。

## 使用方法

管理员进入 **系统设置 → 模型定价**：

1. 已知新模型官方价格时，新增精确的上游模型 ID，并填写完整的输入、输出、
   缓存读取、缓存写入单价；可以分别设置标准、Fast、Flex、长上下文档位。
2. 或点击同步，读取 Sub2API 的 LiteLLM 格式社区价目，先查看变更和跳过的模型，
   确认后才写入。确认时重新获取来源；来源已变化则要求重新预览。
3. 人工覆盖优先于同步价格，同步价格优先于内置价格；按档位合并，不覆盖人工倍率。
4. “重置”移除人工覆盖，恢复同步/内置层，不修改历史账单。

## 后台同步

价格来源固定为：

- JSON：`https://raw.githubusercontent.com/Wei-Shaw/model-price-repo/main/model_prices_and_context_window.json`
- SHA-256：`https://raw.githubusercontent.com/Wei-Shaw/model-price-repo/main/model_prices_and_context_window.sha256`

这是第三方维护的 LiteLLM 格式目录，不是 OpenAI 官方定价 API，也不是订阅额度真实扣费接口。
价目中的别名作为精确模型 ID 导入，不按前缀、相似名字或模型家族猜价。
来源仓库自身可能使用增量同步，因此不能保证已有模型价格总是与厂商最新价目一致。

后台 `pricing_sync:pricing` worker 启动后进行首轮检查，之后每 10 分钟检查一次，不依赖管理员
打开页面。后台提供三个策略：

| 策略 | 行为 |
| --- | --- |
| 自动加入新模型（默认） | 只加入尚无内置、同步或人工价格的模型，不自动调整已有价目 |
| 自动加入并更新已有价格 | 按来源更新同步层的完整档位，不覆盖人工层 |
| 仅手动同步 | 不进行后台网络请求，管理员仍可手动预览并确认 |

同步不删除来源移除的模型或缺失档位，保留最后一次已验证价格。更改策略写入 PostgreSQL，
不需要重启；策略在网络请求期间变化时放弃该轮提交，下轮重新判断。
相同内容不重复提交配置修订或审计事件。页面展示最近成功处理的来源时间，失败记录在服务日志中。

LiteLLM 的单价单位为 USD / Token，转换时使用精确十进制运算，转为 USD / 百万 Token，
不通过浮点乘法换算。标准、Priority/Fast、Flex、长上下文分别映射完整档位；
OpenAI 长上下文阈值为 272k，xAI 为 200k，不把未知阈值套到现有规则。
未知的档位不会凭空套用标准价；新模型长上下文缺少完整标准长上下文档位时也保留未知。
缓存价格缺失、超出精度、额外音频/reasoning 定价或上下文规则无法表达的条目会跳过，
不能将缺失项当作免费或者普通输入价格。

同步只接受固定 HTTPS 地址，禁用跳转，总超时 30 秒，JSON 响应上限 32 MiB；
没有变化时只下载摘要。SHA-256 不匹配时拒绝应用，保留已有价格并等待重试；
摘要是完整性检查，不是独立可信方的数字签名。不提供任意 URL 抓取，也不在推理请求中联网获取价格。
下载、校验或解析失败不会清空数据库中的现有配置，不阻断服务启动。

## 费用和权限不变量

- PostgreSQL 是价格权威来源，使用原有 config revision 和运行快照发布机制。
- 请求开始时冻结价格；重试沿用同一份价格，不受请求进行中的改价影响。
- 已结算请求保存当时的价格/费用明细，历史费用不随当前价目重算。
- 原有请求时分组模型倍率、用户权限、共享订阅账本继续生效。模型价格倍率不替换分组倍率。
- Provider 明确上报的真实费用仍优先于本地估算，不用估算覆盖上游事实。
- 价格未知仍是“未知”，不是“免费”。内置 Sol/Luna 价格仅解决这些模型以后的请求，
  不自动补扣或猜测已有未知费用记录。
- 价格 CRUD / 同步只开放给管理员；普通用户不能修改全局价目。

## 管理接口

全部使用原有管理员会话或管理员 API Key：

| 方法 | 路径 | 含义 |
| --- | --- | --- |
| GET | `/api/admin/settings/pricing` | 内置、同步和人工价格及同步时间 |
| POST | `/api/admin/settings/pricing/update` | 批量设置、改倍率或重置人工覆盖 |
| POST | `/api/admin/settings/pricing/sync/preview` | 只读获取同步预览 |
| POST | `/api/admin/settings/pricing/sync` | 确认预览并持久化同步层 |
| POST | `/api/admin/settings/pricing/sync/policy` | 保存 `{ "enabled": true, "updateExisting": false }` 同步策略 |

价目响应增加 `syncPolicy` 与 `sourceHash`；预览包含 `prices`、`skipped` 和 `sourceHash`，
确认时须原样提交完整预览，服务端重新检查来源。自动同步使用系统审计身份，人工操作保留管理员身份。

单价为非负十进制字符串，最多四位小数（USD / 百万 Token），零必须显式配置。
倍率使用基点，`10000` 为 1 倍。

```json
{
  "provider": "openai",
  "models": ["new-model"],
  "change": {
    "action": "replace",
    "pricing": {
      "multiplierBps": 10000,
      "bands": {
        "standard": {
          "input": "2",
          "output": "10",
          "cacheRead": "0.2",
          "cacheWrite": "2.5"
        }
      }
    }
  }
}
```

示例只是接口形状，不是对任意 `new-model` 的价格承诺。新增迁移为
`0025_model_pricing.sql` 和 `0026_pricing_sync.sql`；0001–0024 的迁移文件没有改动。
