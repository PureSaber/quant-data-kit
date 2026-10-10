# 衍生品研究数据包 v1

期货和普通期权共用 `qdk.derivatives/v1`：`manifest.json` 记录来源、用途、限制和 SHA-256；`contracts.json` 是明确的合约规则；`quotes.csv` 是带时区和可用时间的行情。目的目录不可覆盖。只读校验不生成行情或研究结果。

```powershell
python -m quant_data_kit.derivatives.cli demo --kind future --output demo-futures
python -m quant_data_kit.derivatives.cli demo --kind option --output demo-options
python -m quant_data_kit.derivatives.cli validate --bundle demo-options
```

演示数据包含两个到期月份，期权含三个执行价及看涨/看跌。价格、日历、成交量、保证金均为合成假设，不能据此证明策略有效。

## 合约和行情

合约必需字段：`instrument_id, symbol, product, venue, currency, kind, multiplier, tick, listed_at, last_trade_at, expiry, known_at, timezone, settlement`。时间必须包含时区；`timezone` 为 IANA 时区；币种为三位大写代码。期权另需 `underlying, underlying_kind, option_right, strike, exercise_style`。`initial_margin, maintenance_margin, rules_source` 明确保证金和规则来源。一个包只包含一个合约规则版本；不同历史规则区间分包处理。

CSV 的列顺序见 `Quote` 数据类或演示文件：`instrument_id,at,available_at,session,open,high,low,close,volume,open_interest,settlement,bid,ask,underlying_price`。可选字段为空；价格和金额使用十进制字符串。重复、乱序、未知合约、不一致 OHLC、过期生命周期、NaN、负期权权利金都拒绝。期货数据允许负价用于分析，但当前共享回放只支持正价成交和结算。

`available_at` 表示研究程序能够知道该条观察的最早时间；`known_at` 表示能知道合约规则的时间。补填为历史时间仅是明确假设，不能冒充历史证据。当前包只允许 `synthetic` 或 `retrospective`，不产生 PIT 认证。

## CSV 与 API

```powershell
python -m quant_data_kit.derivatives.cli import-csv --contracts contracts.json --quotes quotes.csv --output imported --rights-note "my licensed research data" --limits "retrospective rules and availability assumptions"
python -m quant_data_kit.derivatives.cli fetch --provider dataway --contracts contracts.json --start 2025-01-02 --end 2025-01-10 --output captured --rights-note "authorized internal research"
python -m quant_data_kit.derivatives.cli fetch --provider databento --dataset GLBX.MDP3 --max-cost-usd 0.25 --contracts contracts.json --start 2025-01-02 --end 2025-01-03 --output captured-db --rights-note "licensed personal research"
```

- Dataway：管理员设置 `DATAWAY_BASE_URL` 为已有授权网关；不把内部网络地址或密钥写入仓库。只读逐日请求，日期按单日查询定义。空结果不登记为成功。
- Databento：本地环境变量 `DATABENTO_API_KEY`；先请求费用估算，超过用户填写的上限就停止，下载不自动重试。估算上限不是供应商硬性扣费保证。只允许明确合约 `raw_symbol`，不把连续合约当作可交易合约。
- 每次限 1–40 个合约、1–32 天，每个响应限 32 MiB；长历史分批采集后按统一规则规范化。
- 不假设接口有交易所保证金或完整生命周期。Databento OHLCV 不含本接口需要的结算价，因此期货只能先做行情分析；期权缺标的价格时需补齐。API 数据可用时间保守设为次日 UTC 零点并标为回溯假设。
- 原始响应保留摘要哈希，私有原始市场文件留在本机。规则和价格的真实性由提供者负责；工程校验不授予数据许可。

API 依据：[Databento Historical API](https://databento.com/docs/api-reference-historical?historical=http)。
