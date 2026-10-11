import json
import multiprocessing as mp
import os
import sqlite3
import subprocess
from pathlib import Path

import pytest

from quant_data_kit.artifacts import ArtifactRef, ArtifactStore
from quant_data_kit.exceptions import ValidationError


def _publish_worker(root, start, queue):
    start.wait(30)
    store = ArtifactStore(Path(root))
    ref = store.put_bytes(b"same-content" * 100000, owner=f"worker:{os.getpid()}")
    queue.put(ref.to_dict())


def _killable_publish(root, ready):
    from quant_data_kit import artifacts

    store = ArtifactStore(Path(root))
    replace = artifacts.os.replace

    def pause_after_replace(source, destination):
        replace(source, destination)
        ready.set()
        # The parent kills this real process before any SQLite commit occurs.
        import time

        time.sleep(120)

    artifacts.os.replace = pause_after_replace
    store.put_bytes(b"interrupted-publication", owner="must-not-exist")


def _mutation_publish(root, source, copied, resume, queue):
    store = ArtifactStore(Path(root))
    original = store._put_stream

    def pause_after_copy(*args):
        result = original(*args)
        copied.set()
        resume.wait(30)
        return result

    store._put_stream = pause_after_copy
    try:
        store.put_file(Path(source), owner="must-not-exist")
    except ValidationError as exc:
        queue.put(str(exc))


def _retain_or_delete(root, ref_json, start, queue, action):
    store = ArtifactStore(Path(root))
    ref = ArtifactRef.from_dict(ref_json)
    start.wait(30)
    try:
        if action == "retain":
            store.retain("race", [ref])
        else:
            store.delete(ref)
        queue.put((action, "ok"))
    except ValidationError:
        queue.put((action, "rejected"))


def _object_path(store, ref):
    return store.root / "objects" / ref.sha256[:2] / ref.sha256


def test_ref_contract_and_reject_arbitrary_uris():
    value = {
        "schema_version": "quant.artifact-ref/v1",
        "sha256": "a" * 64,
        "size_bytes": 0,
        "uri": "qdk+sha256:" + "a" * 64,
    }
    assert ArtifactRef.from_dict(value).to_dict() == value
    for changes in (
        {"uri": "file:///etc/passwd"},
        {"uri": "../../outside"},
        {"uri": "qdk+sha256:" + "b" * 64},
        {"sha256": "A" * 64},
        {"size_bytes": True},
        {"size_bytes": -1},
        {"schema_version": "v0"},
        {"unexpected": "field"},
    ):
        with pytest.raises(ValidationError):
            ArtifactRef.from_dict({**value, **changes})
    value.pop("schema_version")
    with pytest.raises(ValidationError):
        ArtifactRef.from_dict(value)


def test_dedup_copy_and_transactional_pins(tmp_path):
    store = ArtifactStore(tmp_path / "store")
    source = tmp_path / "input"
    source.write_bytes(b"original")
    first = store.put_file(source, owner="job:1")
    second = store.put_bytes(b"original", owner="job:2", media_type="text/plain")
    assert first.sha256 == second.sha256
    assert len(list((store.root / "objects").glob("*/*"))) == 1
    source.write_bytes(b"changed")
    assert store.read_bytes(first) == b"original"
    with pytest.raises(ValidationError, match="protected"):
        store.delete(first)
    assert store.collect(min_age_seconds=0) == []
    store.release("job:1")
    assert store.collect(min_age_seconds=0) == []
    store.release("job:2")
    assert store.collect(min_age_seconds=0) == [first.sha256]
    with pytest.raises(ValidationError, match="missing"):
        store.verify(first)


def test_pin_failure_rolls_back_all_pins(tmp_path):
    store = ArtifactStore(tmp_path)
    ref = store.put_bytes(b"exists")
    missing = ArtifactRef("a" * 64, 1, "qdk+sha256:" + "a" * 64)
    with pytest.raises(ValidationError):
        store.retain("owner", [ref, missing])
    assert store.release("owner") == 0
    store.retain("owner", [ref, ref])
    store.retain("owner", [ref])
    assert store.release("owner") == 1


def test_corruption_rejected_on_read_and_dedup(tmp_path):
    store = ArtifactStore(tmp_path)
    ref = store.put_bytes(b"contents")
    _object_path(store, ref).write_bytes(b"mutated!")
    with pytest.raises(ValidationError, match="integrity"):
        store.read_bytes(ref)
    with pytest.raises(ValidationError, match="integrity"):
        store.put_bytes(b"contents")
    with pytest.raises(ValidationError, match="integrity"):
        store.retain("broken", [ref])


def test_store_and_source_traversal_rejected(tmp_path):
    with pytest.raises(ValidationError, match="traversal"):
        ArtifactStore(tmp_path / ".." / "outside")
    store = ArtifactStore(tmp_path / "store")
    with pytest.raises(ValidationError, match="traversal"):
        store.put_file(tmp_path / "x" / ".." / "input")
    with pytest.raises(ValidationError, match="regular"):
        store.put_file(tmp_path)


@pytest.mark.parametrize("kind", ["root", "ancestor", "source", "object", "database", "lock"])
def test_symlinks_rejected(tmp_path, kind):
    outside = tmp_path / "outside"
    outside.mkdir()
    source = outside / "source"
    source.write_bytes(b"untouched")
    store = ArtifactStore(tmp_path / "store")
    ref = store.put_bytes(b"untouched")
    link = tmp_path / "link"
    target, is_dir = (outside, True) if kind in {"root", "ancestor"} else (source, False)
    if kind == "object":
        link = _object_path(store, ref)
        link.unlink()
    if kind in {"database", "lock"}:
        link = store.root / ("index.sqlite3" if kind == "database" else "store.lock")
        link.unlink()
    try:
        link.symlink_to(target, target_is_directory=is_dir)
    except OSError as exc:
        pytest.skip(f"Host cannot create symlinks: {exc}")
    with pytest.raises(ValidationError, match="Symlink/reparse"):
        if kind == "root":
            ArtifactStore(link)
        elif kind == "ancestor":
            ArtifactStore(link / "child")
        elif kind == "source":
            store.put_file(link)
        else:
            store.read_bytes(ref)
    assert source.read_bytes() == b"untouched"


def test_hardlinked_object_rejected(tmp_path):
    store = ArtifactStore(tmp_path / "store")
    ref = store.put_bytes(b"original")
    (tmp_path / "external-hardlink").hardlink_to(_object_path(store, ref))
    with pytest.raises(ValidationError, match="hard links"):
        store.verify(ref)


@pytest.mark.skipif(os.name != "nt", reason="Windows directory junctions")
@pytest.mark.parametrize("kind", ["root", "ancestor", "source", "object-shard"])
def test_real_windows_junctions_rejected(tmp_path, kind):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "source").write_bytes(b"untouched")
    store = ArtifactStore(tmp_path / "store")
    ref = store.put_bytes(b"untouched")
    link = tmp_path / "junction"
    if kind == "object-shard":
        link = _object_path(store, ref).parent
        _object_path(store, ref).unlink()
        link.rmdir()
        (outside / ref.sha256).write_bytes(b"untouched")
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(link), str(outside)],
        check=True,
        capture_output=True,
    )
    try:
        with pytest.raises(ValidationError, match="Symlink/reparse"):
            if kind == "root":
                ArtifactStore(link)
            elif kind == "ancestor":
                ArtifactStore(link / "child")
            elif kind == "source":
                store.put_file(link / "source")
            else:
                store.read_bytes(ref)
        assert (outside / "source").read_bytes() == b"untouched"
    finally:
        # rmdir removes the junction itself and never recurses into its target.
        link.rmdir()


def test_real_multiprocess_identical_publication(tmp_path):
    ctx = mp.get_context("spawn")
    start, queue = ctx.Event(), ctx.Queue()
    workers = [
        ctx.Process(target=_publish_worker, args=(str(tmp_path), start, queue)) for _ in range(4)
    ]
    try:
        for process in workers:
            process.start()
        start.set()
        refs = [queue.get(timeout=60) for _ in workers]
        for process in workers:
            process.join(30)
            assert process.exitcode == 0
        assert len({json.dumps(ref, sort_keys=True) for ref in refs}) == 1
        assert len(list((tmp_path / "objects").glob("*/*"))) == 1
        assert ArtifactStore(tmp_path).collect(min_age_seconds=0) == []
    finally:
        for process in workers:
            if process.is_alive():
                process.terminate()
            process.join(10)


def test_killed_publication_is_not_visible_and_can_be_reclaimed(tmp_path):
    ctx = mp.get_context("spawn")
    ready = ctx.Event()
    process = ctx.Process(target=_killable_publish, args=(str(tmp_path), ready))
    try:
        process.start()
        assert ready.wait(45)
    finally:
        process.terminate()
        process.join(15)
    store = ArtifactStore(tmp_path)  # OS lock released when process died.
    with sqlite3.connect(tmp_path / "index.sqlite3") as db:
        assert db.execute("SELECT count(*) FROM objects").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM pins").fetchone()[0] == 0
    assert len(store.collect(min_age_seconds=0)) == 1
    assert list((tmp_path / "objects").glob("*/*")) == []
    ref = store.put_bytes(b"interrupted-publication", owner="recovered")
    assert store.read_bytes(ref) == b"interrupted-publication"


def test_real_source_change_during_import_is_rejected(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"before")
    ctx = mp.get_context("spawn")
    copied, resume, queue = ctx.Event(), ctx.Event(), ctx.Queue()
    process = ctx.Process(
        target=_mutation_publish,
        args=(
            str(tmp_path / "store"),
            str(source),
            copied,
            resume,
            queue,
        ),
    )
    try:
        process.start()
        assert copied.wait(45)
        source.write_bytes(b"after-and-a-different-size")
        resume.set()
        assert "Source changed" in queue.get(timeout=30)
        process.join(15)
        assert process.exitcode == 0
        store = ArtifactStore(tmp_path / "store")
        assert store.release("must-not-exist") == 0
        assert len(store.collect(min_age_seconds=0)) == 1
    finally:
        if process.is_alive():
            process.terminate()
        process.join(10)


def test_real_pin_delete_race_has_one_safe_winner(tmp_path):
    store = ArtifactStore(tmp_path)
    ref = store.put_bytes(b"race")
    ctx = mp.get_context("spawn")
    start, queue = ctx.Event(), ctx.Queue()
    workers = [
        ctx.Process(
            target=_retain_or_delete,
            args=(
                str(tmp_path),
                ref.to_dict(),
                start,
                queue,
                action,
            ),
        )
        for action in ("retain", "delete")
    ]
    try:
        for process in workers:
            process.start()
        start.set()
        results = dict(queue.get(timeout=60) for _ in workers)
        assert sorted(results.values()) == ["ok", "rejected"]
        for process in workers:
            process.join(15)
            assert process.exitcode == 0
        if results["retain"] == "ok":
            store.verify(ref)
            assert store.collect(min_age_seconds=0) == []
        else:
            assert store.release("race") == 0
    finally:
        for process in workers:
            if process.is_alive():
                process.terminate()
            process.join(10)


@pytest.mark.parametrize("age", [-1, float("nan"), float("inf")])
def test_gc_invalid_age(tmp_path, age):
    with pytest.raises(ValidationError):
        ArtifactStore(tmp_path).collect(min_age_seconds=age)


def test_context_log_contains_utc_event(tmp_path, caplog):
    with caplog.at_level("INFO", logger="quant_data_kit.artifacts"):
        ArtifactStore(tmp_path, context={"run_id": "run:1"}).put_bytes(b"x")
    record = caplog.records[-1]
    assert record.event == "artifact_published"
    assert record.context == {"run_id": "run:1"}
    assert record.timestamp.endswith("+00:00")
