import json
import multiprocessing as mp
import os
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_data_kit import ArtifactRef, ArtifactStore, SnapshotStore, create_snapshot
from quant_data_kit.exceptions import ValidationError


def _snapshot_worker(root, source, start, queue):
    start.wait(30)
    manifest = SnapshotStore(ArtifactStore(Path(root))).publish(
        {"prices": Path(source)},
        dataset="prices",
        version="1",
        temporal={"timezone": "UTC"},
        owner=f"worker:{os.getpid()}",
        available_at_column="available_at",
    )
    queue.put(manifest.to_dict())


def _killable_snapshot(root, source, ready):
    from quant_data_kit import artifacts

    snapshots = SnapshotStore(ArtifactStore(Path(root)))
    original = artifacts.os.replace
    count = 0

    def pause_manifest(source, destination):
        nonlocal count
        original(source, destination)
        count += 1
        if count == 2:
            ready.set()
            import time

            time.sleep(120)

    artifacts.os.replace = pause_manifest
    snapshots.publish(
        {"prices": Path(source)},
        dataset="prices",
        version="1",
        temporal={"timezone": "UTC"},
        owner="killed",
        available_at_column="available_at",
    )


def _gc_worker(root, started, done):
    started.set()
    ArtifactStore(Path(root)).collect(min_age_seconds=0)
    done.set()


@pytest.fixture
def fixture_table(tmp_path):
    frame = pd.DataFrame(
        {
            "date": [
                "2024-01-01",
                "2024-01-01",
                "2024-01-02",
                "2024-01-02",
                "2024-01-03",
                "2024-01-03",
            ],
            "symbol": ["A", "B", "A", "B", "A", "B"],
            "close": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0],
            "available_at": pd.to_datetime(
                [
                    "2024-01-01T18:00:00.000000001Z",
                    "2024-01-01T18:00:00.000000002Z",
                    "2024-01-02T18:00:00.000000001Z",
                    "2024-01-02T18:00:00.000000002Z",
                    "2024-01-03T18:00:00.000000001Z",
                    "2024-01-03T18:00:00.000000002Z",
                ]
            ),
            "unused": [b"big" * 1000] * 6,
        }
    )
    table = pa.Table.from_pandas(frame, preserve_index=False)
    path = tmp_path / "source.parquet"
    pq.write_table(table, path, row_group_size=2)
    return frame, table, path


def _publish(root, source, **kwargs):
    store = SnapshotStore(ArtifactStore(root))
    options = {
        "dataset": "prices",
        "version": "1",
        "temporal": {"timezone": "UTC", "calendar": "fixture"},
        "owner": "run:1",
        "available_at_column": "available_at",
    }
    options.update(kwargs)
    return store, store.publish({"prices": source}, **options)


def test_query_matches_independent_full_frame_filter(tmp_path, fixture_table):
    frame, table, _path = fixture_table
    # Two physical members exercise schema binding and deterministic partition order.
    left, right = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pq.write_table(table.slice(0, 3), left)
    pq.write_table(table.slice(3), right)
    snapshots = SnapshotStore(ArtifactStore(tmp_path / "store"))
    manifest = snapshots.publish(
        {"b": right, "a": left},
        dataset="prices",
        version="1",
        temporal={"timezone": "UTC"},
        owner="run:1",
        available_at_column="available_at",
    )
    cutoff = "2024-01-03T18:00:00.000000001Z"
    batches = list(
        snapshots.iter_batches(
            manifest.artifact,
            columns=["symbol", "close"],
            start_date="2024-01-02",
            end_date="2024-01-03",
            symbols=["A"],
            as_of=cutoff,
            batch_size=2,
        )
    )
    expected = frame.loc[
        frame.date.between("2024-01-02", "2024-01-03")
        & frame.symbol.isin(["A"])
        & (frame.available_at <= pd.Timestamp(cutoff)),
        ["symbol", "close"],
    ].reset_index(drop=True)
    pd.testing.assert_frame_equal(pa.Table.from_batches(batches).to_pandas(), expected)
    assert all(batch.num_rows <= 2 for batch in batches)
    pd.testing.assert_frame_equal(snapshots.read_table(manifest.artifact).to_pandas(), frame)
    assert snapshots.load(manifest.artifact).snapshot_id == manifest.snapshot_id
    assert manifest.artifact.schema_version == "quant.artifact-ref/v1"


def test_projection_streams_without_full_table_materialization(
    tmp_path, fixture_table, monkeypatch
):
    _, _, path = fixture_table
    snapshots, manifest = _publish(tmp_path / "store", path)
    original = pq.ParquetFile.iter_batches
    calls = []

    def spy(self, *args, **kwargs):
        calls.append(kwargs)
        yield from original(self, *args, **kwargs)

    def forbidden(*args, **kwargs):
        raise AssertionError("Full-table read attempted by streaming query")

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", spy)
    monkeypatch.setattr(pq.ParquetFile, "read", forbidden)
    monkeypatch.setattr(pq, "read_table", forbidden)
    monkeypatch.setattr(pd, "read_parquet", forbidden)
    with closing(
        snapshots.iter_batches(
            manifest.artifact,
            columns=["close"],
            symbols=["A"],
            batch_size=1,
        )
    ) as batches:
        assert next(batches).to_pydict() == {"close": [10.0]}
    assert calls == [{"batch_size": 1, "columns": ["close", "symbol"]}]


def test_snapshot_identity_and_source_mutation(tmp_path, fixture_table):
    _, _, source = fixture_table
    snapshots, manifest = _publish(tmp_path / "store", source)
    elsewhere = tmp_path / "copy.parquet"
    elsewhere.write_bytes(source.read_bytes())
    _, identical = _publish(tmp_path / "store", elsewhere, owner="run:2")
    assert identical.artifact == manifest.artifact
    _, other_store = _publish(tmp_path / "elsewhere-store", elsewhere)
    assert other_store.artifact == manifest.artifact
    _, version = _publish(tmp_path / "store", source, version="2")
    _, temporal = _publish(
        tmp_path / "store", source, temporal={"timezone": "UTC", "calendar": "changed"}
    )
    assert len({manifest.snapshot_id, version.snapshot_id, temporal.snapshot_id}) == 3
    source.write_bytes(b"mutated source")
    assert snapshots.read_table(manifest.artifact).num_rows == 6
    # One Parquet object and three metadata objects; pins do not duplicate content.
    assert len(list((tmp_path / "store" / "objects").glob("*/*"))) == 4


def test_snapshot_pin_protects_members_until_all_owners_release(tmp_path, fixture_table):
    _, _, source = fixture_table
    snapshots, manifest = _publish(tmp_path / "store", source)
    artifacts = snapshots.artifacts
    member = ArtifactRef.from_dict(manifest.document["members"][0]["artifact"])
    artifacts.retain("job:consumer", [manifest.artifact])
    artifacts.release("run:1")
    assert artifacts.collect(min_age_seconds=0) == []
    with pytest.raises(ValidationError, match="protected"):
        artifacts.delete(member)
    artifacts.release("job:consumer")
    assert set(artifacts.collect(min_age_seconds=0)) == {manifest.artifact.sha256, member.sha256}


def test_snapshot_hash_corruption_checked_before_first_batch(tmp_path, fixture_table):
    _, _, source = fixture_table
    snapshots, manifest = _publish(tmp_path / "store", source)
    member = manifest.document["members"][0]["artifact"]
    path = snapshots.artifacts.root / "objects" / member["sha256"][:2] / member["sha256"]
    path.write_bytes(b"bad parquet bytes")
    with pytest.raises(ValidationError, match="integrity"):
        next(snapshots.iter_batches(manifest.artifact, columns=["close"], batch_size=1))
    with pytest.raises(ValidationError, match="integrity"):
        snapshots.artifacts.verify(manifest.artifact)
    with pytest.raises(ValidationError, match="integrity"):
        snapshots.artifacts.retain("new-consumer", [manifest.artifact])
    assert snapshots.artifacts.release("new-consumer") == 0


def test_row_order_duplicates_and_partition_order_are_preserved(tmp_path, fixture_table):
    _, table, _ = fixture_table
    # Deliberately reverse dates and duplicate a row. Data layer must not reorder
    # market records or silently collapse a revision chosen by the research layer.
    table = table.take(pa.array([4, 0, 0, 3]))
    first, second = tmp_path / "one.parquet", tmp_path / "two.parquet"
    pq.write_table(table.slice(0, 2), first)
    pq.write_table(table.slice(2), second)
    snapshots = SnapshotStore(ArtifactStore(tmp_path / "store"))
    options = {
        "dataset": "ordered",
        "version": "1",
        "temporal": {"timezone": "UTC"},
        "owner": "run:order",
        "available_at_column": "available_at",
    }
    forward = snapshots.publish({"a": first, "b": second}, **options)
    reversed_mapping = snapshots.publish({"b": second, "a": first}, **options)
    assert forward.artifact == reversed_mapping.artifact
    assert snapshots.read_table(forward.artifact).equals(table)


def test_unregistered_manifest_cannot_claim_pinned_dependencies(tmp_path, fixture_table):
    _, _, source = fixture_table
    snapshots, manifest = _publish(tmp_path / "store", source)
    document = manifest.document.copy()
    document["version"] = "forged"
    forged = snapshots.artifacts.put_bytes(json.dumps(document).encode(), owner="forger")
    with pytest.raises(ValidationError, match="not registered"):
        snapshots.load(forged)


@pytest.mark.parametrize(
    "query",
    [
        {"columns": ["missing"]},
        {"columns": ["close", "close"]},
        {"columns": []},
        {"start_date": "2024-1-1"},
        {"start_date": "2024-01-03", "end_date": "2024-01-02"},
        {"batch_size": 0},
        {"batch_size": True},
        {"as_of": "2024-01-03"},
        {"as_of": "2024-01-03T00:00:00.0000000001Z"},
        {"symbols": "A"},
    ],
)
def test_invalid_queries_fail_closed(tmp_path, fixture_table, query):
    _, _, source = fixture_table
    snapshots, manifest = _publish(tmp_path / "store", source)
    with pytest.raises((ValidationError, ValueError)):
        list(snapshots.iter_batches(manifest.artifact, **query))


def test_empty_selection_keeps_projected_schema(tmp_path, fixture_table):
    _, _, source = fixture_table
    snapshots, manifest = _publish(tmp_path / "store", source)
    table = snapshots.read_table(manifest.artifact, columns=["symbol", "close"], symbols=[])
    assert table.num_rows == 0
    assert table.column_names == ["symbol", "close"]
    assert table.schema.field("close").type == pa.float64()


@pytest.mark.parametrize("date_kind", ["date", "timestamp"])
def test_typed_date_end_is_inclusive(tmp_path, fixture_table, date_kind):
    frame, _, source = fixture_table
    values = pd.to_datetime(frame.date, utc=True)
    frame.date = values.dt.date if date_kind == "date" else values + pd.Timedelta(hours=23)
    frame.to_parquet(source, index=False)
    snapshots, manifest = _publish(tmp_path / "store", source)
    table = snapshots.read_table(manifest.artifact, start_date="2024-01-02", end_date="2024-01-02")
    assert table.num_rows == 2


def test_pit_nanosecond_cutoff_excludes_next_nanosecond(tmp_path, fixture_table):
    _, _, source = fixture_table
    snapshots, manifest = _publish(tmp_path / "store", source)
    actual = snapshots.read_table(
        manifest.artifact, columns=["symbol"], as_of="2024-01-01T18:00:00.000000001Z"
    )
    assert actual.to_pydict() == {"symbol": ["A"]}


@pytest.mark.parametrize(
    "change",
    ["null-availability", "naive-availability", "invalid-date", "bad-parquet", "schema-mismatch"],
)
def test_invalid_members_not_published(tmp_path, fixture_table, change):
    frame, _, source = fixture_table
    store = SnapshotStore(ArtifactStore(tmp_path / "store"))
    if change == "null-availability":
        frame.loc[0, "available_at"] = pd.NaT
    elif change == "naive-availability":
        frame.available_at = frame.available_at.dt.tz_localize(None)
    elif change == "invalid-date":
        frame.loc[0, "date"] = "2024-1-1"
    frame.to_parquet(source, index=False)
    files = {"prices": source}
    if change == "bad-parquet":
        source.write_bytes(b"not parquet")
    if change == "schema-mismatch":
        second = tmp_path / "other.parquet"
        frame.drop(columns="unused").to_parquet(second, index=False)
        files["second"] = second
    with pytest.raises(ValidationError):
        store.publish(
            files,
            dataset="x",
            version="1",
            temporal={"UTC": True},
            owner="bad",
            available_at_column="available_at",
        )
    assert store.artifacts.release("bad") == 0
    store.artifacts.collect(min_age_seconds=0)
    assert list((store.artifacts.root / "objects").glob("*/*")) == []


def test_corrupt_non_filter_column_is_rejected_at_publication(tmp_path, fixture_table):
    _, _, source = fixture_table
    with pq.ParquetFile(source) as parquet:
        offset = parquet.metadata.row_group(0).column(2).data_page_offset
    with source.open("r+b") as stream:
        stream.seek(offset)
        stream.write(b"\xff" * 16)  # Damage close's page header, preserving footer/roles.
    with pytest.raises(ValidationError, match="Invalid Parquet"):
        _publish(tmp_path / "store", source)


def test_legacy_snapshot_import_preserves_and_checks_hashes(tmp_path):
    frame = pd.DataFrame({"date": ["2024-01-02"], "symbol": ["A"], "close": [10.0]})
    legacy = create_snapshot(
        frame, tmp_path / "legacy", dataset="prices", source="fixture", as_of="2024-01-03"
    )
    path = tmp_path / "legacy" / "prices" / legacy.snapshot_id / "manifest.json"
    store = SnapshotStore(ArtifactStore(tmp_path / "store"))
    manifest = store.import_snapshot(path, owner="legacy")
    assert manifest.document["temporal"]["legacy_snapshot"]["as_of"] == "2024-01-03"
    pd.testing.assert_frame_equal(store.read_table(manifest.artifact).to_pandas(), frame)
    with pytest.raises(ValidationError, match="availability"):
        store.read_table(manifest.artifact, as_of="2024-01-03T00:00:00Z")
    frame.close = [11.0]
    frame.to_parquet(path.parent / "data.parquet", index=False)
    with pytest.raises(ValidationError, match="content/schema"):
        store.import_snapshot(path, owner="changed")
    assert store.read_table(manifest.artifact).to_pydict()["close"] == [10.0]


@pytest.mark.parametrize(
    "data_path", ["../outside.parquet", "/absolute.parquet", "C:/absolute.parquet"]
)
def test_legacy_escaping_path_rejected(tmp_path, data_path):
    frame = pd.DataFrame({"date": ["2024-01-02"], "symbol": ["A"], "close": [10.0]})
    legacy = create_snapshot(frame, tmp_path, dataset="prices", source="fixture")
    path = tmp_path / "prices" / legacy.snapshot_id / "manifest.json"
    payload = json.loads(path.read_text())
    payload["data_path"] = data_path
    path.write_text(json.dumps(payload))
    with pytest.raises(ValidationError):
        SnapshotStore(ArtifactStore(tmp_path / "store")).import_snapshot(path, owner="bad")


def test_real_concurrent_snapshot_publication(tmp_path, fixture_table):
    _, _, source = fixture_table
    ctx = mp.get_context("spawn")
    start, queue = ctx.Event(), ctx.Queue()
    root = tmp_path / "store"
    workers = [
        ctx.Process(target=_snapshot_worker, args=(str(root), str(source), start, queue))
        for _ in range(3)
    ]
    try:
        for process in workers:
            process.start()
        start.set()
        manifests = [queue.get(timeout=60) for _ in workers]
        assert len({item["snapshot_id"] for item in manifests}) == 1
        for process in workers:
            process.join(20)
            assert process.exitcode == 0
        assert len(list((root / "objects").glob("*/*"))) == 2
        ref = ArtifactRef.from_dict(manifests[0]["artifact"])
        assert SnapshotStore(ArtifactStore(root)).read_table(ref).num_rows == 6
    finally:
        for process in workers:
            if process.is_alive():
                process.terminate()
            process.join(10)


def test_killed_snapshot_publication_recovers_without_dangling_pins(tmp_path, fixture_table):
    _, _, source = fixture_table
    ctx = mp.get_context("spawn")
    ready = ctx.Event()
    root = tmp_path / "store"
    process = ctx.Process(target=_killable_snapshot, args=(str(root), str(source), ready))
    try:
        process.start()
        assert ready.wait(45)
    finally:
        process.terminate()
        process.join(15)
    assert ArtifactStore(root).release("killed") == 0
    assert len(ArtifactStore(root).collect(min_age_seconds=0)) == 2
    snapshots, manifest = _publish(root, source)
    assert snapshots.read_table(manifest.artifact).num_rows == 6


def test_live_iterator_holds_gc_lock_until_closed(tmp_path, fixture_table):
    _, _, source = fixture_table
    snapshots, manifest = _publish(tmp_path / "store", source)
    snapshots.artifacts.release("run:1")
    ctx = mp.get_context("spawn")
    started, done = ctx.Event(), ctx.Event()
    process = ctx.Process(target=_gc_worker, args=(str(snapshots.artifacts.root), started, done))
    iterator = snapshots.iter_batches(manifest.artifact, batch_size=1)
    try:
        assert next(iterator).num_rows == 1
        process.start()
        assert started.wait(45)
        assert not done.wait(0.3)
        assert next(iterator).num_rows == 1
        iterator.close()
        assert done.wait(30)
        process.join(15)
        assert process.exitcode == 0
    finally:
        iterator.close()
        if process.is_alive():
            process.terminate()
        process.join(10)


def test_real_cli_publish_query_and_bad_uri(tmp_path, fixture_table):
    _, _, source = fixture_table
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")}
    command = [
        sys.executable,
        "-m",
        "quant_data_kit.artifact_cli",
        "--root",
        str(tmp_path / "store"),
    ]
    published = subprocess.run(
        command
        + [
            "publish",
            "--member",
            f"prices={source}",
            "--dataset",
            "prices",
            "--version",
            "1",
            "--temporal-json",
            '{"timezone":"UTC"}',
            "--owner",
            "cli",
            "--available-at-column",
            "available_at",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
        check=True,
    )
    ref_file = tmp_path / "ref.json"
    ref_file.write_text(published.stdout, encoding="utf-8")
    queried = subprocess.run(
        command
        + [
            "query",
            "--ref",
            str(ref_file),
            "--columns",
            "close",
            "--symbols",
            "A",
            "--end-date",
            "2024-01-02",
            "--batch-size",
            "1",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
        check=True,
    )
    assert [json.loads(row) for row in queried.stdout.splitlines()] == [
        {"close": 10.0},
        {"close": 12.0},
    ]
    payload = json.loads(published.stdout)
    payload["artifact"]["uri"] = str(source)
    ref_file.write_text(json.dumps(payload))
    rejected = subprocess.run(
        command + ["verify", "--ref", str(ref_file)],
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert rejected.returncode == 2
    assert "URI" in rejected.stderr
