import json

import pytest

from quant_data_kit.artifacts import ArtifactStore, canonical_json
from quant_data_kit.exceptions import ValidationError
from quant_data_kit.revision_impact import RevisionRegistry, json_changes, main


def test_transitive_impact_preserves_original_and_pins_dependencies(tmp_path):
    store = ArtifactStore(tmp_path / "s")
    registry = RevisionRegistry(store)
    old = store.put_bytes(b'{"price":"100"}')
    new = store.put_bytes(b'{"price":"110"}')
    feature = store.put_bytes(b'{"feature":"1"}')
    forecast = store.put_bytes(b'{"forecast":"2"}')
    separate = store.put_bytes(b'{"unrelated":true}')
    for output, source, stage in [
        (feature, old, "feature"),
        (forecast, feature, "forecast"),
        (separate, new, "unrelated"),
    ]:
        registry.register(
            output=output,
            inputs={"input": source},
            stage=stage,
            controls={"fixed": True},
            code_identity={"producer": "a" * 40},
            owner=stage,
        )
    report = registry.impact(old, new)
    assert {x["stage"] for x in report["affected_objects"]} == {"feature", "forecast"}
    assert not registry.impact(old, old)["affected_objects"]
    assert store.read_bytes(old) == b'{"price":"100"}'
    with pytest.raises(ValidationError, match="referenced|protected|pinned"):
        store.delete(old)
    refs = []
    for name, ref in [("old", old), ("new", new)]:
        path = tmp_path / (name + ".json")
        path.write_text(json.dumps(ref.to_dict()), encoding="utf-8")
        refs.append(path)
    out = tmp_path / "impact.json"
    assert (
        main(
            [
                "--store",
                str(store.root),
                "--old-ref",
                str(refs[0]),
                "--new-ref",
                str(refs[1]),
                "--out",
                str(out),
            ]
        )
        == 0
    )
    assert json.loads(out.read_bytes()) == report
    with pytest.raises(FileExistsError):
        main(
            [
                "--store",
                str(store.root),
                "--old-ref",
                str(refs[0]),
                "--new-ref",
                str(refs[1]),
                "--out",
                str(out),
            ]
        )


def test_frozen_pair_and_changed_controls_code_or_other_input_rejected(tmp_path):
    store = ArtifactStore(tmp_path)
    registry = RevisionRegistry(store)
    old, new, control = [store.put_bytes(v) for v in (b"old", b"new", b"control")]
    outputs = [store.put_bytes(canonical_json({"nav": v})) for v in ("100", "102")]

    def register(i, **changes):
        kwargs = {
            "output": outputs[i],
            "inputs": {"data": (old, new)[i], "control": control},
            "stage": "result",
            "controls": {"fee": "1"},
            "code_identity": {"producer": "a" * 64},
            "owner": "pair",
        }
        return registry.register(**{**kwargs, **changes})

    a, b = register(0), register(1)
    pair = registry.paired(a, b, old_input=old, new_input=new)
    assert pair["changes"] == [
        {"path": "/nav", "old": "100", "new": "102", "change": "modified", "delta": "2"}
    ]
    for changes in [
        {"controls": {"fee": "2"}},
        {"code_identity": {"producer": "b" * 64}},
        {"inputs": {"data": new}},
        {"stage": "other"},
    ]:
        different = register(1, **changes)
        with pytest.raises(ValidationError):
            registry.paired(a, different, old_input=old, new_input=new)
    with pytest.raises(ValidationError):
        registry.paired(a, b, old_input=control, new_input=new)


def test_corruption_cycles_and_unknown_identity_fail(tmp_path):
    store = ArtifactStore(tmp_path)
    registry = RevisionRegistry(store)
    a, b = store.put_bytes(b"a"), store.put_bytes(b"b")
    kwargs = {
        "stage": "feature",
        "controls": {},
        "code_identity": {"producer": "a" * 40},
        "owner": "cycle",
    }
    ref = registry.register(output=b, inputs={"data": a}, **kwargs)
    with pytest.raises(ValidationError, match="cyclic"):
        registry.register(output=a, inputs={"data": b}, **kwargs)
    with pytest.raises(ValidationError, match="self-dependent"):
        registry.register(output=a, inputs={"data": a}, **kwargs)
    with pytest.raises(ValidationError, match="code hashes"):
        registry.register(
            output=a, inputs={"data": b}, **{**kwargs, "code_identity": {"producer": "main"}}
        )
    store._object_path(a.sha256).write_bytes(b"corrupted")
    with pytest.raises(ValidationError, match="integrity"):
        registry.load(ref)
    with pytest.raises(ValidationError, match="integrity"):
        registry.impact(a, b)


def test_json_diff_missing_and_bool_are_not_numeric_zero():
    result = json_changes(
        {"a/b": True, "gone": None, "list": [1]}, {"a/b": 1, "added": None, "list": [2]}
    )
    assert result[0] == {"path": "/a~1b", "old": True, "new": 1, "change": "modified"}
    assert result[1]["change"] == "added"
    assert result[2]["change"] == "removed"
    assert result[3]["delta"] == "1"


def test_diamond_reports_all_affected_dependencies_independent_of_order(tmp_path, monkeypatch):
    store = ArtifactStore(tmp_path)
    registry = RevisionRegistry(store)
    a, x, b, z, revised = [
        store.put_bytes(value.encode()) for value in ("a", "x", "b", "z", "revised")
    ]
    records = []
    for output, inputs in [(x, {"a": a}), (b, {"x": x}), (z, {"a": a, "b": b})]:
        records.append(
            registry.register(
                output=output,
                inputs=inputs,
                stage="diamond",
                code_identity={"p": "a" * 40},
                controls={},
                owner="diamond",
            )
        )
    original = registry._all
    monkeypatch.setattr(
        registry,
        "_all",
        lambda db: sorted(original(db), key=lambda pair: pair[1]["output"]["sha256"] != z.sha256),
    )
    impact = registry.impact(a, revised)
    result = next(row for row in impact["affected_objects"] if row["output"]["sha256"] == z.sha256)
    assert result["affected_dependencies"] == sorted([a.sha256, b.sha256])
    monkeypatch.setattr(registry, "_all", lambda db: list(reversed(original(db))))
    assert registry.impact(a, revised) == impact


def test_snapshot_partition_dependency_is_traced(tmp_path):
    import pandas as pd

    from quant_data_kit.shared_snapshots import SnapshotStore

    store = ArtifactStore(tmp_path / "store")
    registry = RevisionRegistry(store)
    path = tmp_path / "bars.parquet"
    pd.DataFrame({"date": ["2026-01-02"], "symbol": ["A"], "price": [100]}).to_parquet(path)
    snapshot = SnapshotStore(store).publish(
        {"bars": path},
        dataset="prices",
        version="v1",
        temporal={"availability": "synthetic"},
        owner="snapshot",
    )
    from quant_data_kit.artifacts import ArtifactRef

    member = ArtifactRef.from_dict(snapshot.document["members"][0]["artifact"])
    output = store.put_bytes(b'{"nav":"100"}')
    registry.register(
        output=output,
        inputs={"snapshot": snapshot.artifact},
        stage="result",
        code_identity={"p": "a" * 40},
        controls={},
        owner="result",
    )
    revised = store.put_bytes(b"new partition")
    report = registry.impact(member, revised)
    assert snapshot.artifact.sha256 in report["affected_artifact_hashes"]
    assert report["affected_objects"][0]["output"] == output.to_dict()
