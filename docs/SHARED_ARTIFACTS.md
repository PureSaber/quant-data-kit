# 共享产物与不可变查询快照v1

`quant_data_kit.ArtifactStore`提供本机内容寻址存储；`SnapshotStore`将同schema的Parquet分区发布为不可变快照。旧`DatasetSnapshot/create_snapshot/load_snapshot`接口保持原样，使用`SnapshotStore.import_snapshot()`导入旧清单。

## Python接口

```python
from pathlib import Path
from quant_data_kit import ArtifactRef, ArtifactStore, SnapshotStore

artifacts = ArtifactStore(Path("./shared-artifacts"), context={"run_id": "run-001"})
snapshots = SnapshotStore(artifacts)
manifest = snapshots.publish(
    {"part-000": Path("prices.parquet")},
    dataset="synthetic-prices",
    version="1",
    temporal={
        "timezone": "UTC",
        "calendar": "synthetic-daily-v1",
        "availability": "available_at <= as_of",
        "adjustment": "none",
    },
    available_at_column="available_at",
    owner="run/run-001/inputs",
)
ref_json = manifest.artifact.to_dict()
# Worker从公共ArtifactRef重新解析，禁止把uri作为文件路径打开。
ref = ArtifactRef.from_dict(ref_json)
artifacts.verify(ref)  # 校验自身以及已登记的全部依赖成员。
artifacts.retain("job/job-001/inputs", [ref])
for batch in snapshots.iter_batches(
    ref,
    columns=["date", "symbol", "close"],
    start_date="2024-01-01",
    end_date="2024-01-31",
    symbols=["A"],
    as_of="2024-02-01T00:00:00Z",
    batch_size=1024,
):
    consume(batch)  # pyarrow.RecordBatch

output = artifacts.put_bytes(
    b'{"score": 1.5}',
    media_type="application/json",
    owner="job/job-001/attempt/attempt-001/outputs",
)
artifacts.retain("job/job-001/attempt/attempt-001/outputs", [output])
# 然后由任务库验证租约/fencing并提交成功。
```

普通文件用`put_file(path, owner=..., media_type=...)`。`put_bytes/put_file`的owner可省略，但省略时仅产生未引用对象；提交任务结果应在发布时提供owner。`retain(owner, refs)`原子、累加、幂等；任意一个引用失败会回滚整个调用。`read_bytes(ref)`显式读入内存；`with artifacts.open(ref) as stream`提供只读二进制流。`snapshots.load(ref)`额外验证快照清单、schema和成员行数。`read_table(ref, **query)`显式物化完整查询结果。

可运行合成示例：

```powershell
python examples/shared_snapshot.py --root ./example-artifacts --output ./example-snapshot.json
python -m quant_data_kit.artifact_cli --root ./example-artifacts inspect --ref ./example-snapshot.json
```

## 身份、清单与解析

ArtifactRef格式为：

```json
{
  "schema_version": "quant.artifact-ref/v1",
  "sha256": "<64位小写十六进制>",
  "size_bytes": 123,
  "uri": "qdk+sha256:<与sha256字段相同>",
  "media_type": "application/vnd.quant.snapshot+json"
}
```

`sha256`始终是所保存原始字节的完整SHA-256，普通文件与Parquet均不重编码。相同字节在一个根目录内只保存一份；media_type属于引用提示，不改变对象身份。URI严格校验为上述形式，不能包含机器路径、URL或相对路径。

快照清单的UTF-8JSON采用键排序、紧凑分隔符、保留Unicode且拒绝非有限JSON数值；不做Unicode归一化或不同数值类型合并。成员按名称排序。清单本身是一个产物，`snapshot_id="sha256-"+manifest.artifact.sha256`；清单内不嵌入自身哈希。其字段包括：

- `schema_version="quant.snapshot-manifest/v1"`、`dataset`、用户提供的`version`；
- `temporal`：调用方明确提供的可得性、日历、调整及时间口径；
- `query_columns`：date_column、symbol_column、available_at_column；
- `arrow_schema`：包含metadata的Arrow IPC schema的Base64；
- `members`：名称、完整ArtifactRef、行数。

默认不加入源绝对路径、当前机器信息或发布墙钟。同一字节、同一schema和口径跨根目录身份相同；更换schema、版本或口径会产生新身份。调用方应避免在temporal里自行加入机器路径。旧快照导入保留原清单的来源、as_of和版本，并对复制后的Parquet执行原有逻辑内容哈希、schema哈希及行列校验；旧清单仅有as_of元数据时不冒充逐行可得性，带as_of的查询会拒绝执行。

## 查询与PIT语义

- `columns=None`选择全部列；明确列表必须非空、无重复且全部存在。过滤列可不在投影列表中，会内部读取但不出现在结果中。
- date列支持Arrow date32/date64、严格YYYY-MM-DD字符串、UTC timestamp；起止日期均包含。UTC timestamp的结束日期包含该日全天。
- symbol列必须为字符串；`symbols=None`表示全部，`symbols=[]`表示空结果；空字符串作为字面证券标识处理，不提供通配符。证券列、日期列、配置的availability列均拒绝null。
- availability列必须是UTC timestamp；`as_of`必须有UTC时区，边界为`available_at <= as_of`，保留纳秒精度。未配置availability列时禁止as_of查询。未传as_of表示读取全部版本，调用方须为PIT研究显式提供截止点。
- 保留行顺序与重复记录：成员名称排序后按各文件物理顺序连接，不自动排序、去重或选择“最新修订”。修订选择、研究日历和历史证券池属于上层语义。
- 输出每批不超过batch_size。先以1MiB缓冲校验所有成员的字节哈希，再逐分区只解码投影及筛选所需列。v1不根据统计量跳过整分区；哈希校验本身仍须读取全部输入字节。测试记录读取列和批大小，禁止查询路径调用read_table、ParquetFile.read或pandas.read_parquet。
- 只支持上述参数，不接受SQL、表达式或uri路径。`temporal`记录调用方的时间口径，不自动认证源数据的经济含义、企业行动或历史证券池真实性。

迭代器从第一次读取到结束持续持有全局存储锁。提前停止时使用`contextlib.closing(snapshots.iter_batches(...))`或显式`close()`；不要在未关闭迭代器时重入同一存储的其他API。此设计保证查询期间GC不能移除成员，但长查询会阻塞其他读写，v1不声明高吞吐并行扫描。

## 引用生命周期、故障与回收

存储使用稳定的OS文件锁协调本机进程；SQLite事务登记对象、owner pin及清单到成员的依赖。发布先把字节写入独立staging文件、flush/fsync，再原子重命名，最后提交索引和引用。快照的全部成员、依赖及owner在同一事务内发布。

发布者进程中断可能留下完整未索引文件或staging文件，但不会暴露部分清单或已提交悬空pin。可重试发布复用相同字节，也可显式GC回收。发布时逐批解码全部列，避免非筛选列的损坏页进入快照；查询时仅解码所需列。读取、retain和verify均校验哈希；retain/verify同时校验已登记的依赖闭包。源文件写后修改不改变已复制对象；导入时通过文件身份、尺寸与同一种stat调用的前后时间观测检测普通并发修改。

运行线采用`job/{job_id}/inputs`与`job/{job_id}/attempt/{attempt_id}/outputs`。顺序为发布且pin→Worker再retain→任务库finish。任务库提交失败或崩溃时允许遗留pin。CAS与任务库没有分布式事务；首版运行线不自动release或GC，故不会因跨库窗口回收已发布结果。存储层不决定fencing或任务权威，不能让旧attempt释放其他attempt的owner。

`release(owner)`只显式撤销该owner全部pin，不删除数据。`delete(ref)`在存在owner pin或清单依赖时拒绝；`collect(min_age_seconds=3600)`仅回收超过宽限期的无引用对象和中断发布文件。清单被回收后才可回收其成员；任何被pin的清单都递归保护成员。保守空间泄漏优于成功记录指向缺失文件；自动按任务终态释放引用是后续范围。

安全范围是可信本机进程与本地文件系统。拒绝词法路径越界、symlink、Windows junction/reparse point，以及存储对象的hardlink；消费者不获得可写文件句柄。不声明抵御拥有存储根写权限的敌对进程制造TOCTOU、不声明网络共享SQLite、多机HA或断电级持久性。POSIX目录额外fsync；Windows原子rename与文件fsync已使用，但突然掉电后的目录持久性依赖文件系统。

日志使用Python logging，包含event、timestamp（UTC）、context（建议run_id）及对象sha256/owner。库不强制配置日志处理器；CLI的stdout只输出JSON或JSONL。

## CLI

安装后使用`qdk-artifacts`，或`python -m quant_data_kit.artifact_cli`。

```powershell
qdk-artifacts --root ./store --run-id run-001 put ./result.json --owner job/job-001/attempt/attempt-001/outputs
qdk-artifacts --root ./store publish --member part0=./prices.parquet --dataset prices --version 1 --temporal-json '{"timezone":"UTC"}' --owner run/run-001/inputs --available-at-column available_at
qdk-artifacts --root ./store query --ref ./snapshot.json --columns date symbol close --symbols A --start-date 2024-01-01 --end-date 2024-01-31 --as-of 2024-02-01T00:00:00Z --batch-size 1024
qdk-artifacts --root ./store verify --ref ./snapshot.json
qdk-artifacts --root ./store retain --ref ./snapshot.json --owner job/job-001/inputs
qdk-artifacts --root ./store import-snapshot ./legacy/manifest.json --owner run/run-001/inputs
# 仅在人工确认引用生命周期后显式执行：
qdk-artifacts --root ./store release --owner run/run-001/inputs
qdk-artifacts --root ./store gc --min-age-seconds 3600
```

发布JSON可写入snapshot.json；verify/inspect/query/retain同时支持纯ArtifactRefJSON和包含artifact字段的发布结果JSON。非法输入退出码2，成功退出码0。CLI无隐式latest或外部路径resolver。
