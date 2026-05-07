<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sportsdataverse-py tests](#sportsdataverse-py-tests)
  - [Live-API gate](#live-api-gate)
  - [Conventions](#conventions)
  - [Markers](#markers)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# sportsdataverse-py tests

Layout matches the package: `tests/<sport>/` mirrors `sportsdataverse/<sport>/`.

## Live-API gate

Tests that hit external services (ESPN, etc.) use the `skip_if_no_live`
decorator from `tests/conftest.py`. They are skipped by default and only
run when `SDV_PY_LIVE_TESTS=1` is set in the environment.

Run all tests (skipping live ones):

```sh
pytest tests/
```

Run all tests including live ones:

```sh
SDV_PY_LIVE_TESTS=1 pytest tests/
```

Run a single subpackage's live tests:

```sh
SDV_PY_LIVE_TESTS=1 pytest tests/wbb/
```

## Conventions

- One test file per source module (`tests/wbb/test_wbb_<name>.py` for
  `sportsdataverse/wbb/wbb_<name>.py`).
- Smoke tests for live-API modules use `@skip_if_no_live` and assert
  shape/columns rather than exact values (ESPN data drifts).
- Heavier processing tests use pytest fixtures (see
  `tests/cfb/test_cfb_pbp.py` for the `generated_cfb_data` fixture
  pattern).

## Markers

- `sequential` — declared in `pytest.ini`. Used for tests that share
  state and must not be parallelised (e.g., when xdist is in play).
