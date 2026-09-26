# 研究历史数据与覆盖预检

`python -m quant_data_kit.research_coverage history.csv --output snapshot --provider vendor --source-uri https://source.example/dataset --license-note "licensed research"`

CSV/Parquet必须包含`domain,symbol,field,effective_at,available_at,value`。证券代码是字符串；两个时间都必须有时区。`effective_at`表示事实生效时间，`available_at`表示当时可获知时间，不能用下载时间替代历史披露时间。基本面值必须有限，universe/status使用小写`true`/`false`。例如status字段tradable=false表示不可交易。

支持fundamentals、universe、status、classification和corporate_actions领域的冻结导入。保留原文件、规范数据和SHA-256。加载验证全部哈希；已发布目录不能覆盖。导入是来源声明，不表示已独立核验供应商或获得全市场历史。

`asof_history`同时按生效和可获知时间筛选修订，`attach_history`在每个中国交易日15:00附加基本面和行业，保留字段披露时间。默认拒绝超过550天的基本面事实。`preflight`列出交易日、预热、字段、有限值和历史覆盖缺口，不填造价格或披露日期。

工作台当前支持固定观察池；带停牌或变动成分的历史会明确阻断回放，直到具备相应逐日执行模型。公司行动执行仍使用行情输入包中的既有actions表，通用历史导入不会自动替换会计事件。
