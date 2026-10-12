# Immutable revision impact

`quant_data_kit.revision_impact.RevisionRegistry` extends the existing local
`ArtifactStore`; it does not replace snapshot IDs, manifests, or SHA256 identities.
Register a completed output, named `ArtifactRef` inputs, stage, exact code hashes,
and frozen controls with `register(..., owner=...)`. Registration atomically pins
the descriptor and its complete dependencies. Descriptors are immutable; a new
revision or control change creates new evidence. Do not release historical pins
while they are required for audit or replay.

`impact(old_input, new_input)` follows registered derivations and existing store
dependency edges, including a Parquet member's enclosing `SnapshotStore`
manifest. It returns affected output references and the complete set of affected
input dependencies after closure. Affected means reachable, not economically
changed, and not causal proof. Unregistered external objects are outside scope.
Every registered descriptor and its dependency closure is integrity checked.

```python
from pathlib import Path
from quant_data_kit.artifacts import ArtifactStore, canonical_json
from quant_data_kit.revision_impact import RevisionRegistry

store = ArtifactStore(Path("research-store"))
registry = RevisionRegistry(store)
source = store.put_bytes(canonical_json({"price": "100"}), owner="study-v1")
result = store.put_bytes(canonical_json({"nav": "1000"}), owner="study-v1")
derivation = registry.register(
    output=result, inputs={"market": source}, stage="account",
    code_identity={"producer": "a" * 40},  # replace with actual verified code identity
    controls={"recipe": "frozen-recipe-v1"}, owner="study-v1",
)
```

The generic registry records producer declarations; it cannot establish what
executable actually ran. `paired(old_derivation, new_derivation, old_input=...,
new_input=...)` requires identical stage, code identity, frozen controls and all
other input references. It returns exact JSON differences, with finite decimal
deltas where applicable. It rejects changed code/controls and multi-input
confounding. Both outputs and their prior decisions remain intact.

The portfolio `revision-study` adapter supplies actual source/runtime hashes,
checks them before and after each execution, reruns the frozen plan through the
continuous QExec account, registers both results, and reports available economic
stage differences. Features and forecasts are explicitly unavailable there.
This is retrospective data-revision sensitivity, not historical OOS certification.

Write old/new `ArtifactRef.to_dict()` JSON files and run the read-only impact CLI:

```bash
python -m quant_data_kit.revision_impact --store research-store \
  --old-ref old-ref.json --new-ref new-ref.json --out impact.json
```

The CLI refuses to overwrite an existing report. Existing artifact deletion/GC
rules continue to apply; no automatic deletion, acquisition, network access or
live execution is added. Revert this additive module and guide to roll back;
leave published artifacts and historical pins intact.
