import io
import json
import multiprocessing as mp
import sqlite3
from pathlib import Path

import pytest

from quant_data_kit.artifacts import ArtifactRef, ArtifactStore, canonical_json
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.input_bundles import InputBundleStore


def object_path(store, ref):
    return store.root / "objects" / ref.sha256[:2] / ref.sha256


def concurrent_publish(root, source, start, queue):
    store = InputBundleStore(ArtifactStore(Path(root)))
    start.wait(20)
    bundle = store.publish({"prices.csv": Path(source)}, owner=f"pid-{mp.current_process().pid}")
    queue.put(bundle.artifact.to_dict())


def interrupted_publish(root, source, ready):
    import time

    store = ArtifactStore(Path(root))
    original = store._pin

    def stop_before_commit(*args):
        original(*args)
        ready.set()
        time.sleep(120)

    store._pin = stop_before_commit
    InputBundleStore(store).publish({"prices.csv": Path(source)}, owner="uncommitted")


def test_reuse_and_transitive_retention_without_per_trial_copies(tmp_path):
    source = tmp_path / "prices.csv"
    source.write_bytes(b"date,close\n2026-01-01,1\n")
    artifacts = ArtifactStore(tmp_path / "cas")
    store = InputBundleStore(artifacts)
    first = store.publish({"prices.csv": source, "same.csv": source}, owner="project")
    second = store.publish({"same.csv": source, "prices.csv": source}, owner="retry")
    assert first.artifact == second.artifact
    usage = store.describe(first.artifact)
    assert usage["logical_input_bytes"] == 2 * source.stat().st_size
    assert usage["unique_input_bytes"] == source.stat().st_size
    assert len(list((artifacts.root / "objects").glob("*/*"))) == 2
    source.unlink()
    with store.open_member(first.artifact, "prices.csv") as stream:
        assert stream.read().startswith(b"date,close")
        assert not stream.writable()
    artifacts.release("project")
    assert artifacts.collect(min_age_seconds=0) == []
    assert store.load(first.artifact).document == first.document
    artifacts.release("retry")
    assert len(artifacts.collect(min_age_seconds=0)) == 2


def test_changed_input_and_metadata_create_new_identity_keep_old(tmp_path):
    source = tmp_path / "input.txt"
    source.write_text("first")
    store = InputBundleStore(ArtifactStore(tmp_path / "cas"))
    first = store.publish({"input": source}, owner="run", metadata={"version": 1})
    source.write_text("second")
    second = store.publish({"input": source}, owner="run", metadata={"version": 1})
    third = store.publish({"input": source}, owner="run", metadata={"version": 2})
    assert len({first.artifact.sha256, second.artifact.sha256, third.artifact.sha256}) == 3
    with store.open_member(first.artifact, "input") as stream:
        assert stream.read() == b"first"


@pytest.mark.parametrize("name", ["../x", "a/b", "a\\b", "", ".", 1])
def test_invalid_names_rejected(tmp_path, name):
    store = InputBundleStore(ArtifactStore(tmp_path / "cas"))
    with pytest.raises(ValidationError):
        store.publish({name: tmp_path / "missing"}, owner="invalid")


def test_missing_files_and_oversized_metadata_never_publish_partial_bundle(tmp_path):
    store = InputBundleStore(ArtifactStore(tmp_path / "cas"))
    source = tmp_path / "a"
    source.write_text("data")
    with pytest.raises(ValidationError):
        store.publish({"a": source, "z": tmp_path / "missing"}, owner="invalid")
    with pytest.raises(ValidationError, match="1 MiB"):
        store.publish({"a": source}, owner="large", metadata={"x": "x" * (1024 * 1024)})
    with sqlite3.connect(store.artifacts.root / "index.sqlite3") as db:
        assert db.execute("SELECT COUNT(*) FROM objects").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM pins").fetchone()[0] == 0


def test_member_tamper_is_rejected_at_load_read_and_retention(tmp_path):
    source = tmp_path / "input"
    source.write_bytes(b"trusted")
    artifacts = ArtifactStore(tmp_path / "cas")
    store = InputBundleStore(artifacts)
    bundle = store.publish({"input": source}, owner="first")
    member = ArtifactRef.from_dict(bundle.document["members"][0]["artifact"])
    object_path(artifacts, member).write_bytes(b"changed")
    with pytest.raises(ValidationError):
        store.load(bundle.artifact)
    with pytest.raises(ValidationError), store.open_member(bundle.artifact, "input"):
        pass
    with pytest.raises(ValidationError):
        artifacts.retain("next", [bundle.artifact])


def test_forged_manifest_or_dependency_edges_rejected(tmp_path):
    source = tmp_path / "input"
    source.write_bytes(b"trusted")
    artifacts = ArtifactStore(tmp_path / "cas")
    store = InputBundleStore(artifacts)
    bundle = store.publish({"input": source}, owner="first")
    forged = dict(bundle.document, metadata={"forged": True})
    ref = artifacts.put_bytes(canonical_json(forged))
    with pytest.raises(ValidationError, match="dependencies"):
        store.load(ref)
    with (
        pytest.raises(ValidationError, match="does not exist"),
        store.open_member(bundle.artifact, "absent"),
    ):
        pass
    for value in ([], {}, {"schema_version": "unknown"}):
        bad = artifacts.put_bytes(json.dumps(value).encode())
        with pytest.raises(ValidationError):
            store.load(bad)
    with artifacts._session() as db:
        duplicate = json.loads(json.dumps(bundle.document))
        duplicate["members"].append(duplicate["members"][0])
        bad = artifacts._put_stream(db, io.BytesIO(canonical_json(duplicate)), None)
    with pytest.raises(ValidationError, match="Duplicate"):
        store.load(bad)


def test_real_concurrent_publish_reuses_one_bundle(tmp_path):
    source = tmp_path / "prices.csv"
    source.write_bytes(b"row,value\n" * 1000)
    root = tmp_path / "cas"
    ArtifactStore(root)
    context = mp.get_context("spawn")
    start, queue = context.Event(), context.Queue()
    children = [
        context.Process(target=concurrent_publish, args=(root, source, start, queue))
        for _ in range(3)
    ]
    for child in children:
        child.start()
    start.set()
    results = [queue.get(timeout=60) for _ in children]
    for child in children:
        child.join(30)
        assert child.exitcode == 0
    assert all(ref == results[0] for ref in results)
    assert len(list((root / "objects").glob("*/*"))) == 2


def test_killed_publisher_never_exposes_partial_bundle_and_restart_succeeds(tmp_path):
    source = tmp_path / "prices.csv"
    source.write_bytes(b"important-data")
    root = tmp_path / "cas"
    ArtifactStore(root)
    context = mp.get_context("spawn")
    ready = context.Event()
    process = context.Process(target=interrupted_publish, args=(root, source, ready))
    process.start()
    try:
        assert ready.wait(30)
        process.kill()
        process.join(15)
        with sqlite3.connect(root / "index.sqlite3") as db:
            assert db.execute("SELECT COUNT(*) FROM pins").fetchone()[0] == 0
            assert db.execute("SELECT COUNT(*) FROM objects").fetchone()[0] == 0
        store = InputBundleStore(ArtifactStore(root))
        bundle = store.publish({"prices.csv": source}, owner="recovered")
        assert store.load(bundle.artifact).artifact == bundle.artifact
        assert store.artifacts.collect(min_age_seconds=0) == []
    finally:
        if process.is_alive():
            process.kill()
        process.join(15)
