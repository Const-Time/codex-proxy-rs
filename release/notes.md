# v3.18.12

- 修复 OpenAI Ultrafast 请求有 Token 用量却没有计算费用的问题；本地计费统一使用对应 Standard 单价：Fast / Priority 为 2 倍，Ultrafast 为 6 倍。
- 倍率覆盖输入、输出、缓存读取及缓存写入，支持长上下文，再应用模型及分组倍率。按实际发出的请求档位结算，上游响应中的 Standard 标签不会抹除请求档位；搜索等工具调用费不随速度档位加倍。
- 远程价目支持同步 Ultrafast 与长上下文 Ultrafast 数据；远程优惠价及旧的独立 Fast / Ultrafast 覆盖不改变固定 2 倍 / 6 倍规则。可通过编辑 Standard 档单价调整对应速度档位。
- 模型定价页展示派生后的 Fast / Ultrafast 单价，使用记录和计费说明补齐 Ultrafast 标签与规则。

本次无需数据库迁移。新规则用于升级后的新请求，不重算或补扣历史账单；xAI 和图像计费保持原规则。
