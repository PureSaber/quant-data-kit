# Financial foundations v1 — 01–06 / 08–10

这是一组证据驱动的离线金融数据契约，不是全市场历史数据库，也不认证输入来源完整性。所有时间戳必须带时区；effective_at 是经济生效时间，available_at 是研究者获知时间。修订不能提前可见。不可将今天的证券名单或修订值直接回填过去。

## 01 单位与因子输入

`financial.units.normalize_trading_units(frame, volume_unit=..., amount_unit=..., currency=..., share_basis="raw", lot_size=...)` 显式把手转换为股、千元/万元转换为币种金额。手必须提供 lot_size；不允许复权股数、不推断流通股数。输出 free_float_shares 必须来自当时已知的原始股本口径。历史交易量均值不是换手率。

## 02 生命周期与历史证券池

`financial.lifecycle.COLUMNS` 是完整事件列清单。事件含稳定 instrument_id、symbol、venue、kind、effective_at、available_at、source、evidence_id、universe_id、successor_id；不适用字段保留空值。支持上市、退市、更名、转板、合并、入池、出池。
`select_universe(events, effective_at, known_at, universe_id=None, holdings=())` 返回 eligible、retained_holdings、unknown_holdings。出池不等于退市；存量持仓必须继续估值/清算。冲突事件和同时重复的活跃代码/交易所映射拒绝处理。它是 instrument_master 的伴随历史，不替代交易规则主表。

## 03 公司行动与总收益

`financial.actions.ActionTerms` 统一拆并股、现金分红权益/支付、碎股现金、换股合并、分拆、供股权分派与行权、终止现金。换股/分拆/权利分派必须提供成本分配比例和目标估值，行权必须显式指定 election_quantity；跨币种、做空权益和税务自动推断不支持。
`financial.returns.total_return_panel(prices, actions, timezone=..., price_basis="raw")` 仅接受原始报价，构造拆股/分红权益的信号指数。假设除息收盘理论再投资，不代表现金到账。复杂或同时多项行动拒绝进入此简化指数，须使用 QExec 账户回放。账本不得再次应用复权价格中的同一行动。

## 04 可交易状态

`financial.status.permission_asof(records, instrument_id, effective_at, known_at)` 返回买卖两侧许可。字段包含 effective_from、effective_to（右开）、available_at、buy_status/sell_status、reason、source、evidence_id。状态为 tradable/blocked/unknown；缺失、过期、冲突一律 unknown。`no_restriction` 不能作为 tradable 的证据。QExec 和港股适配负责将 unknown 变成订单阻断。

## 05 用途日历

`PurposeCalendar(calendar_id, purpose, version, available_at, valid_from, valid_to, open_days, source, evidence_kind)` 不隐式生成工作日。purpose 为 trading/settlement/banking/connect/dealing/confirmation；有效区间、版本获知时间和开日都必须显式。
`CalendarBook([...]).asof(id, purpose, at, on)` 选择当时可见版本，`calendar.advance(on, lag, at=..., purpose=...)` 计算到期日。缺少覆盖直接失败。正 lag 从 on 之后计开日；lag=0 要求 on 本身开放。HK 与基金适配已消费该契约。

## 06 来源冲突与新快照

1. `discrepancies(observations, at=...)` 比较同一证券/字段/生效时间，先处理金额倍数及百分比；币种、复权口径不一致标为不可直接比较。
2. `freeze_snapshot(chosen_records)` 创建每个经济键唯一的治理快照。
3. `adjudicate(case, selected_observation_id=..., resolved_at=..., reviewer=..., rationale=..., evidence_uri=..., prior_snapshot=..., dependencies=...)` 要求人工选择已有证据，计算受影响节点的传递闭包。
4. `publish_decision(decision, new_path)` 独占创建裁决文件；`apply_decision_snapshot(parent, decision, new_path)` 实际生成修正数据快照，不改旧文件，修正值从裁决时间起可见。
5. 重算受影响的因子/订单/报告是单独动作；发布裁决不等于重算完成。quant-report-hub 可核验并展示待重算清单。

快照和裁决有内容哈希，不等于数字签名/审批权限控制。调用者仍须保管完整来源、旧版本和运行依赖图。

## 07 股息来源发布时间与生命周期v2

`financial.EvidenceTimingV2`以`puresaber.evidence-timing/2`显式区分精确带offset时间、来源本地分钟、发布日期、绝对区间和未知精度。名义区间保存上下端点及开闭；排他上界用`at_or_after`准入，闭上界用`strictly_after`准入，不添加微秒或其他epsilon。分钟先用有证据的offset，或IANA zone加fold/offset，解析为唯一绝对锚点，再在绝对时间线上执行截断或舍入推导。日期分别解析相邻两个本地午夜，因此23/25小时日合法，歧义或不存在的午夜不会被猜测。

被标为evidenced的zone、显示规则、日期语义、区间和clock accuracy必须引用`SourceMaterialReferenceV2`。材料包含冻结版本、SHA-256、定位、审查引用和实际获得时间；QDK只验证结构闭合，不认证调用方ID、发布者或归档机构。材料不完整时`build_evidence_timing_v2()`产生带稳定降级原因的合法`captured_only`；严格`from_dict()`拒绝残缺或矛盾的evidenced声明。物理clock accuracy为unknown不冒充物理边界，也不单独推翻材料完整的名义来源模型。

名义时间只在调用方同时选择`study_mode="retrospective"`和`trust_source_declared_time`时可用。自然前向准入取本地capture、修订/迁移和所用材料实际获得时间的最大值，后来取得的材料不能回填旧运行。`trusted_archive`允许清单由外层治理维护，不写入QDK。

`financial.DividendLifecycleV2`是独立`puresaber.dividend-lifecycle/2`类型，完整覆盖proposal、entitlement、payment election、issuer conversion、payment policy和payment的金额、币种、账户、顺序及现金恒等式。它不继承v1，旧消费者的`isinstance(DividendLifecycle)`不会接纳它。`migrate_dividend_lifecycle_v1_to_v2()`只接受实际规范v1 JSON字节，重新解析并计算source fingerprint，绑定嵌入v1 payload、每个timing JSON pointer、旧available/captured值和迁移规则；普通v2对象不能手写`legacy_available_at`。v1类型、解析器、规范字节和指纹保持不变。

## 08 SEC 独立季度、TTM、重述

`us_research.sec.quarterly_facts(facts, at)` 按实际 fiscal period 边界识别直接季度、同年度累计差分；不按自然季度硬切，不把累计数字当独立季度。保留 accession 与 vintage_consistent。
`ttm_facts(..., allow_mixed_vintages=False, max_age_days=200)` 要求四个连续季度，兼容 53 周年，拒绝缺口及未确认可比的跨文件差分。允许混合版本必须显式 opt-in；这不构成重述可比性认证。
`ttm_quality` 要求同一截止期 USD 净利润/经营现金流和期末资产；ROA 分母是期末资产，不是平均资产。美股 quality_ttm 单独启用，旧 annual 不更名。
真实小样本位于 tests/fixtures/financial/apple_cash_flow_extract.json：Apple 三份 2024 财年 10-Q 的公开现金流与申报索引事实摘录。它验证累计差分及 acceptance+5min 边界，不是下载快照或全市场回测。SEC 下载仍需用户自己的真实联系身份，未写入凭据。

## 09 宏观 vintage

`financial.macro.from_alfred(observations, series_id=..., unit=..., release_times=..., source=...)` 接受存档 ALFRED JSON。realtime_start 只有日期精度，不能猜成午夜或默认 08:30；release_times 必须显式提供有证据的带时区发布时间。同一系列不能隐式改变单位。`macro_asof` 按当时已知版本选择，缺失点不填充。quant-regime 输出上下文、缺失和陈旧状态，不暗中改变原状态分类器。

## 10 基金/ETF 穿透

`financial.holdings.COLUMNS` 定义 disclosure_id、fund_id、holding_date、available_at、instrument_id、asset_type、currency、weight、source、evidence_id。披露是完整快照，不把后来缺失的持仓与旧披露拼接。
`look_through(disclosures, {fund_id: "portfolio_weight"}, at, max_depth=8, max_age_days=180)` 使用 Decimal 并保留每条路径。未披露余额、陈旧、缺失、循环和深度上限都变为 UNKNOWN，不丢弃权重；exposure_summary 聚合重叠证券与币种。现金必须使用 CASH:<currency>。
仅支持 long-only 权重；不自动推断衍生品 delta、杠杆、汇率或债券发行人集团敞口。披露穿透不是实时持仓。金额输入须先用已知 FX 转为统一组合权重。

## 使用与回归

安装本分支后执行：
```powershell
python -m quant_data_kit.financial.cli --input examples/financial/units.json --output units-result.json
python -m pytest tests/test_financial_foundations.py tests/test_financial_evidence.py
```
CLI schema 为 puresaber.financial-foundations/1，operation 支持 units/lifecycle/universe/status/calendar/reconcile/sec-ttm/macro/lookthrough。结果明确标注 supplied_facts_not_market_coverage，拒绝覆盖输出文件。

## 参考与边界

- [SEC 官方 API 与 XBRL](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)：申报版本、单位、期间与 acceptance 来源。
- [ALFRED realtime period](https://fred.stlouisfed.org/docs/api/fred/realtime_period.html)：知识时点与修订，不等于日内发布时间。
- [HKEX 结算](https://www.hkex.com.hk/Services/Settlement-and-Depository/Settlement?sc_lang=en)：交易和交收用途分离。
- [LEAN corporate actions](https://www.quantconnect.com/docs/v2/writing-algorithms/securities/asset-classes/us-equity/corporate-actions)：行动事件与复权口径分离，未复制第三方实现。
- [Apple IR dividend/split history](https://investor.apple.com/dividend-history/)：4-for-1 拆股条款；测试价格/持仓及保守获知时间为场景值，不冒充真实逐笔证据。

这些模块不提供收益承诺、投资建议或实盘下单能力；真实数据授权、完整覆盖及历史可获得性仍需使用者验证。
