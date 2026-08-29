# M6 data-foundation governance validation

Date: 2026-08-29

## Scope

- Candidate branch: `codex/cross-asset-v2-m6-governance`
- Baseline: `origin/main@960db6d7f30eae942efd46a0dab7596585277823`
- Package version: `0.6.1`
- Workspace layer: `data`
- Published schemas: `puresaber.market-events@2.0.0`,
  `puresaber.instrument-master@2.0.0`
- Lock: `requirements.lock`
- AKShare remains an explicit optional extra and is absent from the audited lock.

## Lock generation and compatibility

Generated with pip-tools7.6.1:

```text
pip-compile --extra dev --build-deps-for editable --allow-unsafe --strip-extras \
  --resolver backtracking --index-url https://pypi.org/simple \
  --constraint requirements-constraints.txt \
  --output-file requirements.lock pyproject.toml
```

The lock contains one exact version for every base runtime, development, transitive, and editable
build dependency. Linux wheel resolution dry-runs succeeded for CPython3.10,3.11, and3.12 using
the `manylinux2014_x86_64` and `manylinux_2_28_x86_64` compatibility tags.

- `requirements.lock`SHA-256:
  `b9490b718853c8d8f440885bb9b9c1ed15d1980230db1dc2242456fddebbfadd`
- Python3.10 target resolution: pass
- Python3.11 target resolution: pass
- Python3.12 target resolution: pass

## Isolated installation

Executed in a newly created local `.venv` with Python3.12.5:

```text
python -m venv .venv
.venv/Scripts/python -m pip install -r requirements.lock
.venv/Scripts/python -m pip check
.venv/Scripts/python -m pip install -e . --no-deps --no-build-isolation
.venv/Scripts/python -m pip check
```

Both `pip check` calls reported `No broken requirements found`; installed package version was
`0.6.1`.

## Quality gates

```text
ruff check src tests tools
ruff format --check src tests tools
coverage run --branch -m pytest -q --junitxml=validation/m6-governance/pytest-junit.xml
coverage json -o validation/m6-governance/coverage.json
python tools/check_branch_coverage.py validation/m6-governance/coverage.json
coverage report
git diff --check
```

Results:

- Ruff lint: pass.
- Ruff format check: pass,59 files unchanged.
- Pytest:177 passed,1 existing platform skip,0 failures,0 errors.
- Existing skip: POSIX symbolic-link regression on Windows; no tests or skip markers were changed.
- Full combined coverage:89%.
- All source pure branches:935/1164=80.33%, required80%.
- `data_lake.py`:315/350=90.00%.
- `curated.py`:94/100=94.00%.
- `process_lock.py`:8/8=100.00%.
- `schemas_v2.py`:103/108=95.37%.
- `l2_replay.py`:49/54=90.74%.
- `adapters_v2/base.py`:20/22=90.91%.
- `adapters_v2/binance.py`:19/20=95.00%.
- `adapters_v2/okx.py`:36/40=90.00%.
- `adapters_v2/cn_neutral.py`:10/10=100.00%.
- `git diff --check`: pass.

Raw evidence:

- `validation/m6-governance/pytest-junit.xml`
- `validation/m6-governance/coverage.json`
