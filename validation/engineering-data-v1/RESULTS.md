# 数据主线验证记录

日期：2026-10-11。分支：`codex/engineering-data-v1`。基线：`a871ea00e17f8e1de37c3233dc973f0ae9a752ff`。

这是数据仓开发验证，待协调方进行独立验收；不代表三主线整体验收通过。

## 环境与复现

Windows、Python3.12.5，独立环境`.engineering/parallel-v1-20261011/data-venv`。使用本仓原有requirements.lock，未更改依赖或冻结环境。锁文件SHA-256：`7aca9d148c36edb63d13046cafaca85a559eca67266133f8ee931450d0da6fd1`。

```powershell
python -m pip install -r requirements.lock
python -m pip install -e . --no-deps --no-build-isolation
python -m pip check
python -m ruff check src tests tools examples/shared_snapshot.py
python -m ruff format --check src tests tools examples/shared_snapshot.py
python -m coverage run --data-file=validation/engineering-data-v1/.coverage --branch --source=quant_data_kit -m pytest -q --junitxml=validation/engineering-data-v1/full.xml
python -m coverage run --append --data-file=validation/engineering-data-v1/.coverage --branch --source=quant_data_kit -m pytest tests/test_artifacts.py tests/test_shared_snapshots.py tests/test_snapshots.py -q --junitxml=validation/engineering-data-v1/focused-final.xml
python -m coverage json --data-file=validation/engineering-data-v1/.coverage -o validation/engineering-data-v1/coverage.json
python tools/check_branch_coverage.py validation/engineering-data-v1/coverage.json
```

包版本仍为0.8.1，本批以Git commit固定源码。实际导入位置为当前独立worktree的`src/quant_data_kit/__init__.py`。依赖pyarrow25.0.1、pandas2.3.3；pip check无依赖问题。

## 结果

|检查|结果|
|---|---|
|完整仓库回归|977 passed、8 skipped，226.93秒|
|最后修复后的数据专项复验|55 passed、6 skipped，17.03秒|
|Ruff检查与格式|通过，168个检查文件|
|既有纯分支覆盖率门槛|所有核心门槛通过；全仓5101/6348=80.36%，要求80%|
|安装后CLI与合成示例|通过；发布、查询、严格URI拒绝均使用真实进程|
|git diff --check|通过|

完整回归执行期间追加了两项数据回归用例，并将新快照发布校验改为逐批解码全部列，防止非筛选列的损坏页漏检。随后重跑完整受影响数据专项，并追加到覆盖率证据；旧业务模块未改变。最终可复现总收集量为987项，完整回归与新增两项的结果分别记录，不把未经整轮重跑的数目写成全仓执行结果。

真实进程证据包括：4进程相同字节并发写入只留1份；3进程相同快照发布只留数据及清单各1份；发布者在rename后、SQLite提交前被终止，索引与pin均未暴露；清单发布中断后回收并重试；另一个进程修改正在导入的源文件被拒绝；retain与delete竞争只允许一个安全赢家；GC被活跃查询迭代器阻塞，关闭后继续。

查询对照独立pandas全量筛选结果，覆盖多分区、字段投影、日期/证券/PIT组合、空证券选择、UTC日期整日端点、纳秒截止、重复行与物理顺序。查询测试禁止完整表读取API，记录只读取`close/symbol`与batch_size=1。坏Parquet、非筛选列坏页、哈希损坏、旧快照逻辑/schema变化、任意URI、路径越界、hardlink均拒绝。

跳过情况：新增6项symlink用例因Windows缺少创建权限（WinError1314）跳过；4项真实Windows junction用例通过。完整仓库另有1项POSIX专属symlink用例及1项可选quant_pipeline导入用例跳过。POSIX符号链接实跑和跨仓可选用例不计为本环境通过；跨仓集成由协调方单独验收。

原始日志保存在此目录，因含本机临时路径未纳入Git。验收方可直接读取或重新运行；摘要校验如下：

|日志|SHA-256|
|---|---|
|full-tests.log|141c917123cc002ce0e6259168efed01a6c1492a54739c749271bfee0a824d4d|
|focused-final.log|e1654da05376057e02f9a2897f335582ecaa1d90cda850efafbd71f490e46205|
|coverage-gates.log|1f6a83b89496e80b524851584c28d4ab16468e2a74565c751bdc0021aa7863c2|

适用限制见`docs/SHARED_ARTIFACTS.md`：可信本机、本地文件系统、全局锁串行、需显式as_of、哈希读取全部字节、无分区剪枝；旧快照导入可全量物化；无跨任务库事务或自动引用释放，无断电级持久性承诺。
