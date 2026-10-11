"""Immutable derivation evidence and revision reachability over ArtifactStore.

Reachability is limited to registered objects. It is not causal attribution and
does not infer dependencies from filenames, mutable aliases, or coincident IDs.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path

from quant_data_kit.artifacts import ArtifactRef, ArtifactStore, canonical_json
from quant_data_kit.exceptions import ValidationError

SCHEMA = "quant.research-derivation/v1"


def sha256(value):
    return hashlib.sha256(canonical_json(value)).hexdigest()


def _identity(code):
    if (
        not isinstance(code, dict)
        or not code
        or any(
            not isinstance(k, str)
            or not k
            or not isinstance(v, str)
            or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", v)
            for k, v in code.items()
        )
    ):
        raise ValidationError("exact named code hashes required")


class RevisionRegistry:
    def __init__(self, store: ArtifactStore):
        self.store = store
        with store._session() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS research_derivations ("
                "descriptor_sha TEXT PRIMARY KEY REFERENCES objects(sha) ON DELETE CASCADE, "
                "output_sha TEXT NOT NULL REFERENCES objects(sha))"
            )

    def register(self, *, output, inputs, stage, code_identity, controls, owner):
        """Atomically pin a derivation and all input/output dependencies.

        Code/control values are declarations from the producer; consumers must
        verify the actual executable identity separately when running a replay.
        Multiple histories producing identical bytes remain distinct derivations.
        """
        _identity(code_identity)
        if not isinstance(stage, str) or not stage.strip() or not isinstance(controls, dict):
            raise ValidationError("stage and explicit controls object required")
        if (
            not isinstance(inputs, dict)
            or not inputs
            or any(not isinstance(k, str) or not k.strip() for k in inputs)
        ):
            raise ValidationError("named immutable inputs required")
        if any(r.sha256 == output.sha256 for r in inputs.values()):
            raise ValidationError("self-dependent output forbidden")
        document = {
            "schema": SCHEMA,
            "stage": stage,
            "output": output.to_dict(),
            "inputs": {k: v.to_dict() for k, v in sorted(inputs.items())},
            "code_identity": code_identity,
            "controls": controls,
            "controls_sha256": sha256(controls),
        }
        with self.store._session() as db:
            dependencies = [output, *inputs.values()]
            for ref in dependencies:
                self.store._verify_closure(db, ref)
            # Reject cycles in the semantic output -> input graph, not just in
            # ArtifactStore's descriptor -> artifact dependency edges.
            known = self._all(db)
            pending = {r.sha256 for r in inputs.values()}
            visited = set()
            while pending:
                node = pending.pop()
                if node == output.sha256:
                    raise ValidationError("cyclic derivation")
                if node in visited:
                    continue
                visited.add(node)
                for _, item in known:
                    if item["output"]["sha256"] == node:
                        pending.update(r["sha256"] for r in item["inputs"].values())
            descriptor = self.store._put_stream(
                db, io.BytesIO(canonical_json(document)), "application/vnd.quant.derivation+json"
            )
            for ref in dependencies:
                db.execute(
                    "INSERT OR IGNORE INTO edges VALUES (?, ?)", (descriptor.sha256, ref.sha256)
                )
            db.execute(
                "INSERT OR IGNORE INTO research_derivations VALUES (?, ?)",
                (descriptor.sha256, output.sha256),
            )
            self.store._pin(db, owner, [descriptor])
        return descriptor

    def _load(self, db, ref):
        self.store._verify_closure(db, ref)
        value = json.loads(self.store._verify(db, ref).read_bytes())
        if value.get("schema") != SCHEMA or value.get("controls_sha256") != sha256(
            value["controls"]
        ):
            raise ValidationError("invalid derivation manifest")
        _identity(value["code_identity"])
        for raw in [value["output"], *value["inputs"].values()]:
            child = ArtifactRef.from_dict(raw)
            if not db.execute(
                "SELECT 1 FROM edges WHERE parent=? AND child=?", (ref.sha256, child.sha256)
            ).fetchone():
                raise ValidationError("unregistered derivation dependency")
            self.store._verify_closure(db, child)
        return value

    def load(self, ref):
        with self.store._session() as db:
            return self._load(db, ref)

    def _all(self, db):
        results = []
        for sha, size in db.execute(
            "SELECT descriptor_sha, size FROM research_derivations "
            "JOIN objects ON descriptor_sha=objects.sha ORDER BY descriptor_sha"
        ).fetchall():
            ref = ArtifactRef(sha, size, f"qdk+sha256:{sha}")
            results.append((ref, self._load(db, ref)))
        return results

    def impact(self, old_input, new_input):
        with self.store._session() as db:
            self.store._verify_closure(db, old_input)
            self.store._verify_closure(db, new_input)
            known = self._all(db)
            edges = db.execute("SELECT parent, child FROM edges").fetchall()
        affected = {old_input.sha256} if old_input.sha256 != new_input.sha256 else set()
        found = {}
        changed = True
        while changed:
            changed = False
            for parent, child in edges:
                if child in affected and parent not in affected:
                    affected.add(parent)
                    changed = True
            for ref, item in known:
                causes = sorted(
                    r["sha256"] for r in item["inputs"].values() if r["sha256"] in affected
                )
                if causes and ref.sha256 not in found:
                    found[ref.sha256] = {
                        "derivation": ref.to_dict(),
                        "stage": item["stage"],
                        "output": item["output"],
                        "affected_dependencies": causes,
                    }
                    affected.add(item["output"]["sha256"])
                    changed = True
        # A node may be discovered before all of its upstream paths. Report
        # dependencies from the completed closure, not first-discovery order.
        for ref, item in known:
            if ref.sha256 in found:
                found[ref.sha256]["affected_dependencies"] = sorted(
                    r["sha256"] for r in item["inputs"].values() if r["sha256"] in affected
                )
        return {
            "schema": "quant.revision-impact/v1",
            "old_input": old_input.to_dict(),
            "new_input": new_input.to_dict(),
            "affected_objects": [found[k] for k in sorted(found)],
            "affected_artifact_hashes": sorted(affected - {old_input.sha256}),
            "scope": "registered immutable dependency graph only",
            "causal_claim": False,
            "historical_results_modified": False,
        }

    def paired(self, old_derivation, new_derivation, *, old_input, new_input):
        """Validate a frozen data-only pair; never waive the normal comparison guard."""
        old, new = self.load(old_derivation), self.load(new_derivation)
        for field in ("stage", "code_identity", "controls", "controls_sha256"):
            if old[field] != new[field]:
                raise ValidationError(f"not a frozen data-only pair: changed {field}")
        expected = {
            name: new_input.to_dict() if ref["sha256"] == old_input.sha256 else ref
            for name, ref in old["inputs"].items()
        }
        if old_input.sha256 not in {r["sha256"] for r in old["inputs"].values()}:
            raise ValidationError("old revision is not an input of the old result")
        if expected != new["inputs"]:
            raise ValidationError("pair changed other inputs or missing revision replacement")
        old_result = json.loads(self.store.read_bytes(ArtifactRef.from_dict(old["output"])))
        new_result = json.loads(self.store.read_bytes(ArtifactRef.from_dict(new["output"])))
        return {
            "schema": "quant.paired-revision/v1",
            "intervention": "data_revision",
            "code_identity": old["code_identity"],
            "controls_sha256": old["controls_sha256"],
            "old_derivation": old_derivation.to_dict(),
            "new_derivation": new_derivation.to_dict(),
            "old_input": old_input.to_dict(),
            "new_input": new_input.to_dict(),
            "old_result": old["output"],
            "new_result": new["output"],
            "changes": json_changes(old_result, new_result),
            "historical_results_modified": False,
            "causal_claim": False,
        }


def json_changes(old, new, path=""):
    """Stable exact JSON differences. Numeric delta only for finite decimal values."""
    if type(old) is type(new) and old == new:
        return []
    if isinstance(old, dict) and isinstance(new, dict):
        changes = []
        for key in sorted(set(old) | set(new)):
            child = path + "/" + key.replace("~", "~0").replace("/", "~1")
            if key not in old or key not in new:
                changes.append(
                    {
                        "path": child,
                        "old": old.get(key),
                        "new": new.get(key),
                        "change": "added" if key not in old else "removed",
                    }
                )
            else:
                changes.extend(json_changes(old[key], new[key], child))
        return changes
    if isinstance(old, list) and isinstance(new, list) and len(old) == len(new):
        return [
            change
            for i, (a, b) in enumerate(zip(old, new))
            for change in json_changes(a, b, f"{path}/{i}")
        ]
    result = {"path": path, "old": old, "new": new, "change": "modified"}
    if not isinstance(old, bool) and not isinstance(new, bool):
        try:
            a, b = Decimal(str(old)), Decimal(str(new))
            if a.is_finite() and b.is_finite():
                result["delta"] = str(b - a)
        except InvalidOperation:
            pass
    return [result]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--old-ref", type=Path, required=True)
    parser.add_argument("--new-ref", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    registry = RevisionRegistry(ArtifactStore(args.store))
    report = registry.impact(
        *[
            ArtifactRef.from_dict(json.loads(path.read_text(encoding="utf-8")))
            for path in (args.old_ref, args.new_ref)
        ]
    )
    with args.out.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, allow_nan=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
