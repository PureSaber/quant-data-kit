# Shared registered research inputs

`quant_data_kit.input_bundles.InputBundleStore` shares registered CSV, JSON, text
and arbitrary file bytes. It does not assign financial, PIT, schema or executable
trust to the content. Parquet market queries continue to use `SnapshotStore`.

```python
from pathlib import Path
from quant_data_kit.artifacts import ArtifactStore
from quant_data_kit.input_bundles import InputBundleStore

cas = ArtifactStore(Path("D:/research/cas"))
bundles = InputBundleStore(cas)
bundle = bundles.publish(
    {"prices.csv": Path("D:/input/prices.csv"), "plan.json": Path("D:/input/plan.json")},
    owner="project/study-1", metadata={"version": 1},
)
with bundles.open_member(bundle.artifact, "plan.json") as stream:
    plan_bytes = stream.read()
print(bundles.describe(bundle.artifact))
```

Use the same ArtifactRef for candidates/retries. Manifests contain logical names
and content references, not source paths. Names cannot contain path separators.
Up to 1000 files and a 1 MiB manifest are accepted. Files stream into CAS; the
complete input set is not loaded into memory during publication.

## Integrity, concurrency and invalidation

The existing CAS process lock and SQLite transaction cover publication, dependency
edges and owner pins. Concurrent identical publications produce one manifest and
one object per distinct byte content. Changing bytes or metadata produces a new
identity; old pinned versions remain. Duplicate imports still read/hash bytes.

Every load/read verifies hashes, sizes and the manifest's registered dependencies.
Missing/modified members fail closed. The existing CAS rejects symlink/reparse
points and hard links, and checks for source mutation during import. Streams are
read-only and hold the store lock against GC until closed. Close them promptly:
v1 readers are serialized. No writable CAS path or hard link is handed to a trial.
Trusted host-user code still has OS privileges; this is not an untrusted sandbox.

## Retention and crash recovery

Owner pins protect manifests and their member closure. Pipeline research batches
add submission/job pins before execution. Failure, cancellation, retry and restart
never release inputs or delete history. There is no automatic expiry or GC.

After auditing active consumers and historical evidence needs, an operator can
explicitly release an owner. `release` removes pins, not bytes. Separate existing
`collect(min_age_seconds=...)` reclaims only unprotected objects after the grace
period. Savings come from reuse, without requiring historic result deletion.

A killed publisher may leave unindexed files but cannot expose a partial bundle.
Restart verifies/reuses complete bytes and publishes atomically. Explicit CAS GC
can reclaim unindexed leftovers after its grace period.

`describe` separates logical member bytes, distinct member bytes and manifest
bytes. Repeated integrity reads still incur I/O. No zero-I/O or streaming factor
computation is claimed. Existing Notebook inputs are not automatically migrated:
handlers must explicitly consume references; unregistered reads stay outside.
