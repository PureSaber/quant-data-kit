# 研究数据准入与统一读取

`quant_data_kit.research_intake`把用户CSV、TSV或Parquet按“原件留存→显式映射→全量质量检查→不可变版本→按用途和实际scope预检”的顺序接入研究工作台。它不填零、不丢行、不去重，也不猜日期格式、时区或单位。`table`只表示可探索的泛型表；通过结构检查不等于获得市场数据认证。

## 契约

下面是`daily_bars`完整示例。`mapping`方向固定为“规范列→原列”；`units`按规范数值列逐列声明。

```json
{
  "schema_version": "qdk.research-intake-contract/v1",
  "kind": "daily_bars",
  "input": {
    "format": "csv",
    "encoding": "utf-8-sig",
    "delimiter": ","
  },
  "mapping": {
    "symbol": "证券代码",
    "date": "交易日期",
    "open": "开盘价",
    "high": "最高价",
    "low": "最低价",
    "close": "收盘价",
    "volume": "成交量"
  },
  "types": {
    "symbol": "string",
    "date": "date",
    "open": "number",
    "high": "number",
    "low": "number",
    "close": "number",
    "volume": "integer"
  },
  "formats": {"date": "%Y-%m-%d"},
  "primary_key": ["symbol", "date"],
  "metadata": {
    "source": "内部授权导出",
    "provider": "vendor-name",
    "units": {
      "open": "CNY",
      "high": "CNY",
      "low": "CNY",
      "close": "CNY",
      "volume": "share"
    },
    "timezone": "Asia/Shanghai",
    "adjustment": "raw"
  },
  "limits": {"max_bytes": 1073741824, "max_rows": 5000000}
}
```

支持的类型是`string`、`integer`、`number`、`boolean`、`date`和`datetime`。文本日期必须在`formats`中给出精确格式；没有偏移的`datetime`还必须声明`metadata.timezone`。CSV/TSV必须明确`encoding`和单字符`delimiter`，支持例如`utf-8`、`utf-8-sig`、`gb18030`和制表符。Parquet的`input`只有`format`。

`daily_bars`必须映射`symbol,date,open,high,low,close`，`volume`和`amount`可选。严格的`daily_bars_research`用途要求`symbol=string`、`date=date`及OHLC为`integer`或`number`，并检查实际使用数值列的单位以及`source/provider/timezone/adjustment`。严格检查始终以实际scope内的`symbol,date`为自然键核验重复和冲突，不因`primary_key`为空或声明成其他列而跳过。

`history`必须映射`symbol,period_end`和至少一个值列。探索时可以接入没有`available_at`的表，但`historical_financial_factor_backtest`要求`period_end=date`、`available_at`真实存在且类型为`datetime`、每个scope内的行非空，并要求`metadata.availability=point_in_time`。缺少历史披露时间的最新截面不能用于历史财务因子回放。

CSV/TSV在交给pandas前先按原始记录校验表头和每行字段数。重复或空表头、字段数与表头不一致都会失败并保留原件及receipt，不允许自动改列名、把首列变成索引或截断多余字段。`inspect-source`只检查返回样本范围内的行结构，并在`structure_validation.rows_checked/complete`中明确范围；`import`会全量扫描。

`daily_bars`和`history`的零行版本会发布为`blocked`且不会替换原有`latest`。泛型`table`可以保留零行探索版本，但报告包含`empty_table`警告。任一严格研究用途的实际scope匹配零行时，`check`返回`empty_research_scope`并拒绝准入。

## CLI

脚本可用已配置的QDK解释器从文件路径直接运行，不要求先把当前代码安装进环境：

```powershell
$python = 'H:\Documents\ChatGPT\temp\puresaber-quant-platform\quant-data-kit\.venv-maintenance\Scripts\python.exe'

& $python src\quant_data_kit\research_intake.py inspect-source `
  --source runs\.intake\incoming\<uuid>\bars.csv `
  --format csv --encoding utf-8-sig --delimiter ',' --max-sample-rows 20

& $python src\quant_data_kit\research_intake.py import `
  --root runs\.intake\catalog --source bars.csv --name cn-bars --contract contract.json

& $python src\quant_data_kit\research_intake.py list --root runs\.intake\catalog
& $python src\quant_data_kit\research_intake.py show --root runs\.intake\catalog --name cn-bars
& $python src\quant_data_kit\research_intake.py show --root runs\.intake\catalog --receipt receipt-...

& $python src\quant_data_kit\research_intake.py check `
  --root runs\.intake\catalog --name cn-bars --purpose daily_bars_research `
  --columns symbol date close --symbols 000001 --start 2026-01-01 --end 2026-06-30

& $python src\quant_data_kit\research_intake.py read `
  --root runs\.intake\catalog --name cn-bars --purpose daily_bars_research `
  --columns symbol date close --limit 100

& $python src\quant_data_kit\research_intake.py diff `
  --root runs\.intake\catalog --name cn-bars --old sha256-... --new sha256-...

& $python src\quant_data_kit\research_intake.py verify-snapshot `
  --snapshot notebook-inputs\cn-bars\sha256-...

& $python src\quant_data_kit\research_intake.py read-snapshot `
  --snapshot notebook-inputs\cn-bars\sha256-... --purpose daily_bars_research `
  --columns symbol date close --limit 0 --max-rows 1000000
```

每次CLI只向stdout写一个JSON对象：

```json
{
  "schema_version": "qdk.research-intake-response/v1",
  "ok": true,
  "action": "read",
  "data": {}
}
```

`inspect-source`固定返回`columns:[{name,type}]`和`rows`，最多20行，并明确`sample_only:true,quality_checked:false`及`structure_validation:{rows_checked,complete}`。CSV/TSV样例按字符串读取，因此证券代码的前导零不会在字段映射前丢失。

`import`的`data`始终含`receipt`。成功还含`version`和`report`；容量、契约或读取失败时含`error`且`version:null`。失败receipt会记录已留存原件的hash和路径。质量问题会发布`status=blocked`的不可变版本供查看，但不会推进`latest`。只有`ready`版本推进`latest`；`--parent`提供乐观并发检查，失败刷新不会替换旧版。

`check`返回`allowed/status/purpose/version_id/scope/integrity/issues/market_certified`。每个问题组含`rule,row,column,value,severity,message,recommendation,affected_count,scope,samples`；`affected_count`来自全量检查，`samples`最多20条。指定字段、标的和日期后会按实际scope重查，`scope.full_scan`准确说明是否覆盖全表和全部字段。

`read`和`read-snapshot`先校验原件、规范Parquet、契约、报告和问题明细的hash，再执行同一scope预检。CLI的`data`含`raw_sha256/normalized_sha256/contract_sha256/report_sha256/check/rows/matched_rows/returned_rows/complete/truncated/max_rows`。默认最多输出1000行；`--limit 0`才输出全部匹配行，截断永远显式标识。完整研究读取应同时设置独立的正数`--max-rows`；匹配行数超过该上限时读取失败，不会部分冒充完整结果。

`diff`按两个版本的主键给出完整`added/removed/changed/unchanged`计数、schema变化、coverage变化和最多20条键样例。没有主键时只能进行完整行多重集的新增/删除比较，不会伪造“变更行”。

## Python API与不可变目录

```python
from quant_data_kit.research_intake import (
    check_dataset,
    check_snapshot,
    diff_versions,
    import_dataset,
    inspect_source,
    list_datasets,
    read_dataset,
    read_snapshot,
    show_dataset,
    verify_snapshot,
)

preview = inspect_source(
    "bars.csv",
    file_format="csv",
    encoding="utf-8-sig",
    delimiter=",",
    max_sample_rows=20,
)
result = import_dataset("runs/.intake/catalog", "bars.csv", "cn-bars", "contract.json")
check = check_dataset(
    "runs/.intake/catalog",
    "cn-bars",
    purpose="daily_bars_research",
    columns=["symbol", "date", "close"],
    symbols=["000001"],
    start="2026-01-01",
    end="2026-06-30",
)
loaded = read_dataset("runs/.intake/catalog", "cn-bars", purpose="daily_bars_research")
frame = loaded.frame
```

根目录布局如下：

```text
catalog/
  raw/sha256-<原件hash>.<format>
  receipts/receipt-<id>.json
  versions/sha256-<版本identity>/
    manifest.json
    contract.json
    report.json
    issues.parquet
    normalized.parquet
    raw/source.<format>
  catalog.json
```

版本目录是自包含快照。Notebook冻结整个目录后可调用`verify_snapshot(directory)`校验manifest identity及所有文件hash，再用`check_snapshot(...)`或`read_snapshot(...)`按用途读取；未安装QDK包的Notebook进程也可调用配置的QDK解释器执行`research_intake.py verify-snapshot/check-snapshot/read-snapshot`，这些只读动作不加载同目录QDK模块。`normalized.parquet`含内部`_qdk_source_row`用于把问题绑定回原始行；公共读取API不会返回该列。原始文件和规范文件始终分离，规范化或被阻断的值不会改写原件。

容量上限按契约或调用参数配置，没有硬编码的一刀切质量阈值。原件超过容量仍保留hash和失败receipt，但不读取、规范化或发布版本。当前实现验证用户声明的契约、内容和用途边界，不验证数据许可，也不宣称交易所、市场或供应商认证。
