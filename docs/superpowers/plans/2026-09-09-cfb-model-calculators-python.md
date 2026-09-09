# CFB Model Calculators (Python) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give sportsdataverse-py a user-facing CFB calculator surface — hand a data frame to `calculate_expected_points(df)` and get model output back, whether the rows came from a real game or were typed by hand to ask a hypothetical — matching what the package already provides for NFL.

**Architecture:** A card-driven engine (`predict_from_card`) validates a caller's frame against the model's own published `features` array and builds the DMatrix in the card's declared order. A derivation layer maps natural user columns onto model feature names, reusing the mapping tables that already exist in `cfb/model_vars.py`. Ten thin `calculate_*` wrappers sit on top. Existing hardcoded feature lists are replaced with card reads.

**Tech Stack:** Python 3.13, polars 1.x, xgboost 3.2.0, pytest, uv.

**Spec:** `docs/superpowers/specs/2026-09-09-cfb-model-calculators-design.md`

## Global Constraints

- **polars 1.x modern API only** — `group_by`, `with_row_index`, `pl.len()`, `map_elements(..., return_dtype=)`, `how="full", coalesce=True`, `str.strip_chars`. Bool masks explicit (`pl.col("c") == True`). Rust regex has **no lookaround**.
- **uv for everything** — `uv run pytest`, `uv run ruff check`, `uv run mypy`. Never bare `python`/`pip`.
- **`uv run` can silently re-lock `uv.lock`** — check `git status` after; never let a lockfile bump ride along.
- **Codegen is never hand-edited.** If any generated file or docstring changes, run `uv run python tools/codegen/generate.py` then `--check` before pushing. CI drift-gates otherwise.
- **Google-style napoleon docstrings** with Args/Returns/Raises/Example on every public function.
- **ID dtype discipline** — fix one dtype per id at the boundary; never paper over with float→Utf8.
- **Never add AI co-author trailers or attribution footers.** Branch + PR; never push `main`.
- **`ERA_BOUNDS = (2006, 2013, 2020)`.** Never restate it — read `era_contract` from the card.

---

## Context an implementer needs

The `cfb_model_artifacts` release now publishes a complete contract for all nine CFB models (done in cfbfastR-cfb-data#79). Each `<model>.card.json` carries an ordered `features` array; five carry an `era_contract` (`fg`, `qbr`, `fd` one-hot; `two_pt`, `xpass` ordinal) with `cuts: [2006, 2013, 2020]`. `MANIFEST.json` carries `model_version` and `ep_class_contract.class_order`.

**The derivation already exists.** `sportsdataverse/cfb/model_vars.py` holds positionally-aligned lists: `ep_start_columns` (pbp names like `start.TimeSecsRem`) against `ep_final_names` (model names like `TimeSecsRem`), and the same for WP. `ep_final_names` is **byte-identical** to the EP card's `features`. What is missing is a public entry point and card-driven validation — not the mapping itself.

**Five hardcoded feature lists duplicate published cards** and are the drift surface this plan removes:

| constant | file | duplicates |
|---|---|---|
| `EP_FEATURES` | `cfb/cfb_fourth_down.py:96` | `ep_model.card.json` |
| `WP_SPREAD_FEATURES` | `cfb/cfb_fourth_down.py:97` | `wp_spread.card.json` |
| `FD_FEATURES` | `cfb/cfb_fourth_down.py:92` | `fd_model.card.json` |
| `TWO_PT_FEATURES` | `cfb/cfb_two_point.py:69` | `two_pt_model.card.json` |
| `CP_FEATURES` / `XPASS_FEATURES` | `cfb/cfb_pbp.py:270,286` | `cfb_cp_model` / `xpass_model` cards |

The NFL analogue to copy for signature shape and docstring style is
`sportsdataverse/nfl/ep_wp.py::calculate_expected_points`.

## File Structure

| File | Responsibility |
|---|---|
| `sportsdataverse/cfb/model_cards.py` (create) | Load + cache a model card; expose its features and era contract. Nothing else. |
| `sportsdataverse/cfb/model_calculators.py` (create) | `predict_from_card`, `derive_model_features`, and the ten `calculate_*` functions. |
| `sportsdataverse/parsed/cfb.py` (modify) | Re-export the ten public functions. |
| `tests/cfb/test_model_cards.py` (create) | Card loading, caching, missing-card behavior. |
| `tests/cfb/test_model_calculators.py` (create) | Validation, derivation, ordering, the oracle round-trip. |

---

### Task 1: Card loader

**Files:**
- Create: `sportsdataverse/cfb/model_cards.py`
- Test: `tests/cfb/test_model_cards.py`

**Interfaces:**
- Produces: `load_model_card(model: str) -> dict`, `card_features(model: str) -> list[str]`, `card_era_contract(model: str) -> dict | None`.

- [ ] **Step 1: Write the failing test**

```python
"""The model card is the contract; nothing may restate it.

Five hardcoded feature lists in this package duplicate a published card's
`features` array. That duplication is the same class of bug as cfb-data#70,
where consumers kept private copies of the era cuts and drifted to a value the
trainer never used. These accessors are what replaces them.
"""

from __future__ import annotations

import json

import pytest

from sportsdataverse.cfb import model_cards


@pytest.fixture(autouse=True)
def _clear_cache():
    model_cards.load_model_card.cache_clear()
    yield
    model_cards.load_model_card.cache_clear()


def _stub(monkeypatch, payload):
    monkeypatch.setattr(model_cards, "_read_card_json", lambda model: payload)


def test_features_come_back_in_the_cards_declared_order(monkeypatch):
    """Feature ORDER is load-bearing for XGBoost -- a sorted or re-derived list
    silently mis-aligns the DMatrix and the model scores garbage."""
    feats = ["TimeSecsRem", "yards_to_goal", "distance", "down_1"]
    _stub(monkeypatch, {"features": feats, "model_type": "ep"})
    assert model_cards.card_features("ep_model") == feats


def test_era_contract_is_returned_when_present(monkeypatch):
    contract = {"encoding": "one_hot", "columns": ["era0", "era1", "era2", "era3"],
                "cuts": [2006, 2013, 2020]}
    _stub(monkeypatch, {"features": ["yards_to_goal"], "era_contract": contract})
    assert model_cards.card_era_contract("fg_model") == contract


def test_a_model_with_no_era_feature_returns_none(monkeypatch):
    """None is CORRECT for ep/wp_naive/wp_spread/cfb_cp_model -- they have no
    era feature. Inventing a contract for them would tell callers otherwise."""
    _stub(monkeypatch, {"features": ["TimeSecsRem"], "era_contract": None})
    assert model_cards.card_era_contract("ep_model") is None


def test_a_card_without_features_is_rejected(monkeypatch):
    """A card with no features validates nothing -- fail loudly, not silently."""
    _stub(monkeypatch, {"model_type": "ep"})
    with pytest.raises(ValueError, match="no features"):
        model_cards.card_features("ep_model")


def test_the_card_is_read_once_and_cached(monkeypatch):
    calls = []

    def counting(model):
        calls.append(model)
        return {"features": ["a"]}

    monkeypatch.setattr(model_cards, "_read_card_json", counting)
    model_cards.card_features("ep_model")
    model_cards.card_features("ep_model")
    assert len(calls) == 1, "card re-read; every calculator call would hit the network"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cfb/test_model_cards.py -q`
Expected: FAIL — `ImportError: cannot import name 'model_cards'`

- [ ] **Step 3: Write minimal implementation**

```python
"""Read the published contract for a CFB model.

Each model in the ``cfb_model_artifacts`` bundle ships a ``<model>.card.json``
carrying the ordered ``features`` array it was trained with and, where the model
consumes one, an ``era_contract``. Reading that contract is what lets a caller's
frame be validated against the artifact itself instead of a list restated in
this package -- the duplication that produced cfb-data#70.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

#: Cards ship beside the boosters in the packaged model directory.
_MODEL_DIR = Path(__file__).resolve().parent / "models"


def _read_card_json(model: str) -> dict[str, Any]:
    """Read ``<model>.card.json`` from the packaged model directory."""
    path = _MODEL_DIR / f"{model}.card.json"
    if not path.exists():
        raise FileNotFoundError(f"no published card for {model!r} at {path}")
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=None)
def load_model_card(model: str) -> dict[str, Any]:
    """Load and cache one model's published card.

    Args:
        model: Bundle stem, e.g. ``"ep_model"`` or ``"wp_spread"``.

    Returns:
        The parsed card.

    Raises:
        FileNotFoundError: When no card ships for that model.

    Example:
        Quick start::

            from sportsdataverse.cfb.model_cards import load_model_card
            load_model_card("ep_model")["features"]
    """
    return _read_card_json(model)


def card_features(model: str) -> list[str]:
    """The model's feature names, in the order it was trained with.

    Order is load-bearing: XGBoost aligns a DMatrix by position, so a sorted or
    re-derived list scores against the wrong columns without erroring.

    Args:
        model: Bundle stem.

    Returns:
        Ordered feature names.

    Raises:
        ValueError: When the card declares no features -- such a card validates
            nothing and must not be treated as a contract.
    """
    feats = load_model_card(model).get("features")
    if not feats:
        raise ValueError(f"card for {model!r} declares no features")
    return list(feats)


def card_era_contract(model: str) -> Optional[dict[str, Any]]:
    """The model's rule-era encoding, or ``None`` when it has no era feature.

    ``None`` is correct for ``ep_model``, ``wp_naive``, ``wp_spread`` and
    ``cfb_cp_model``; inventing a contract for them would tell a caller they take
    an era feature they have never had.

    Args:
        model: Bundle stem.

    Returns:
        The contract dict, or ``None``.
    """
    return load_model_card(model).get("era_contract")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/cfb/test_model_cards.py -q`
Expected: PASS (5 passed)

- [ ] **Step 5: Verify the packaged cards are actually present**

Run:
```bash
ls sportsdataverse/cfb/models/*.card.json | wc -l
uv run python -c "
from sportsdataverse.cfb.model_cards import card_features, card_era_contract
for m in ('ep_model','fg_model','xpass_model'):
    print(f'  {m}: {len(card_features(m))} features, era={bool(card_era_contract(m))}')"
```
Expected: the packaged cards load. **If `fg_model` reports `era=False`, the packaged copies are the stale pre-rebuild ones** — the release was refreshed in cfb-data#79 but this repo's committed copies may lag. Record that in your report; Task 5 refreshes them.

- [ ] **Step 6: Lint, typecheck, lockfile**

Run: `uv run ruff check sportsdataverse/cfb/model_cards.py tests/cfb/test_model_cards.py && uv run mypy sportsdataverse/cfb/model_cards.py && git status --short uv.lock`
Expected: clean, and no `uv.lock` in the status.

- [ ] **Step 7: Commit**

```bash
git add sportsdataverse/cfb/model_cards.py tests/cfb/test_model_cards.py
git commit -m "feat(cfb): read the published model-card contract

Each model in the cfb_model_artifacts bundle ships a card carrying the ordered
features it was trained with and, where it consumes one, an era_contract.
Reading that contract lets a caller's frame be validated against the artifact
instead of a list restated in this package -- the duplication that produced
cfb-data#70, where consumers drifted to an era cut the trainer never used.

card_features() preserves declared order because XGBoost aligns a DMatrix by
position: a sorted list scores against the wrong columns without erroring."
```

---

### Task 2: Card-driven predict

**Files:**
- Create: `sportsdataverse/cfb/model_calculators.py`
- Test: `tests/cfb/test_model_calculators.py`

**Interfaces:**
- Consumes: `card_features` from Task 1.
- Produces: `predict_from_card(df: pl.DataFrame, model: str, booster) -> np.ndarray`.

- [ ] **Step 1: Write the failing test**

```python
"""Validation and ordering for the card-driven predict path."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest
import xgboost as xgb

from sportsdataverse.cfb import model_calculators as mc


def _booster(features):
    X = np.zeros((4, len(features)), dtype=float)
    y = np.array([0, 1, 0, 1])
    dm = xgb.DMatrix(X, label=y, feature_names=list(features))
    return xgb.train({"objective": "binary:logistic", "max_depth": 1}, dm, num_boost_round=1)


def test_missing_columns_are_named_in_one_error(monkeypatch):
    """The failure this surface exists to remove is a caller unable to tell what
    their frame is missing -- so name every absent column, not the first."""
    monkeypatch.setattr(mc, "card_features", lambda m: ["down", "distance", "yards_to_goal"])
    df = pl.DataFrame({"down": [1]})
    with pytest.raises(ValueError) as exc:
        mc.predict_from_card(df, "xpass_model", _booster(["down", "distance", "yards_to_goal"]))
    msg = str(exc.value)
    assert "distance" in msg and "yards_to_goal" in msg
    assert "xpass_model" in msg, "the error must say which model wanted them"


def test_columns_are_ordered_by_the_card_not_the_frame(monkeypatch):
    """A frame in a different column order must score identically. If the
    DMatrix were built from frame order, this silently scores garbage."""
    feats = ["down", "distance", "yards_to_goal"]
    monkeypatch.setattr(mc, "card_features", lambda m: feats)
    b = _booster(feats)
    ordered = pl.DataFrame({"down": [3.0], "distance": [7.0], "yards_to_goal": [42.0]})
    shuffled = ordered.select(["yards_to_goal", "down", "distance"])
    assert mc.predict_from_card(ordered, "xpass_model", b) == pytest.approx(
        mc.predict_from_card(shuffled, "xpass_model", b)
    )


def test_extra_columns_are_ignored(monkeypatch):
    """A real pbp frame carries hundreds of columns the model never saw."""
    feats = ["down", "distance"]
    monkeypatch.setattr(mc, "card_features", lambda m: feats)
    df = pl.DataFrame({"down": [1.0], "distance": [10.0], "play_text": ["irrelevant"]})
    out = mc.predict_from_card(df, "xpass_model", _booster(feats))
    assert len(out) == 1


def test_an_empty_frame_returns_an_empty_result(monkeypatch):
    feats = ["down", "distance"]
    monkeypatch.setattr(mc, "card_features", lambda m: feats)
    df = pl.DataFrame({"down": [], "distance": []}, schema={"down": pl.Float64, "distance": pl.Float64})
    assert len(mc.predict_from_card(df, "xpass_model", _booster(feats))) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cfb/test_model_calculators.py -q`
Expected: FAIL — `ImportError: cannot import name 'model_calculators'`

- [ ] **Step 3: Write minimal implementation**

```python
"""User-facing CFB model calculators.

Hand a data frame to one of these and get model output back, whether the rows
came from a real game or were typed by hand to ask a hypothetical -- the same
shape ``sportsdataverse.nfl`` already provides via
``calculate_expected_points()``.

Every calculator validates its input against the model's OWN published card
rather than a feature list restated here. Five such restatements exist in this
package today and are exactly the drift that produced cfb-data#70.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import polars as pl
from xgboost import DMatrix

from sportsdataverse.cfb.model_cards import card_era_contract, card_features


def predict_from_card(df: pl.DataFrame, model: str, booster: Any) -> np.ndarray:
    """Score ``df`` with ``booster``, validated and ordered by the model's card.

    Args:
        df: Frame carrying at least the model's declared features. Extra
            columns are ignored, so a full pbp frame passes through unchanged.
        model: Bundle stem, used to look up the card and to name the model in
            any error.
        booster: The loaded ``xgb.Booster``.

    Returns:
        The booster's raw predictions.

    Raises:
        ValueError: When any declared feature is absent from ``df``. Every
            missing column is named in one message -- the failure this surface
            exists to remove is a caller unable to tell what their frame lacks.

    Example:
        Quick start::

            from sportsdataverse.cfb.model_calculators import predict_from_card
            predict_from_card(pbp, "xpass_model", booster)
    """
    feats = card_features(model)
    missing = [f for f in feats if f not in df.columns]
    if missing:
        raise ValueError(
            f"{model} needs {len(missing)} column(s) not present: {', '.join(missing)}. "
            f"Its card declares: {', '.join(feats)}"
        )
    # Selected in the CARD's order, never the frame's: XGBoost aligns a DMatrix
    # by position, so frame order would silently score against wrong columns.
    matrix = df.select(feats).to_pandas()
    return booster.predict(DMatrix(matrix, feature_names=feats))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/cfb/test_model_calculators.py -q`
Expected: PASS (4 passed)

- [ ] **Step 5: Lint, typecheck, lockfile**

Run: `uv run ruff check sportsdataverse/cfb/model_calculators.py tests/cfb/test_model_calculators.py && uv run mypy sportsdataverse/cfb/model_calculators.py && git status --short uv.lock`

- [ ] **Step 6: Commit**

```bash
git add sportsdataverse/cfb/model_calculators.py tests/cfb/test_model_calculators.py
git commit -m "feat(cfb): card-driven predict for the calculator surface

predict_from_card validates a caller's frame against the model's published
features and builds the DMatrix in the card's declared order. Order is
load-bearing: XGBoost aligns by position, so a frame in a different column
order would score against the wrong columns without raising.

Missing columns produce one error naming every absent column and the model that
wanted them, rather than a bare XGBoost shape error."
```

---

### Task 3: Era derivation from the card

**Files:**
- Modify: `sportsdataverse/cfb/model_calculators.py`
- Test: `tests/cfb/test_model_calculators.py` (append)

**Interfaces:**
- Consumes: `card_era_contract` from Task 1.
- Produces: `add_era_columns(df: pl.DataFrame, model: str, season: int | None = None) -> pl.DataFrame`.

- [ ] **Step 1: Write the failing test**

```python
def test_ordinal_era_is_derived_from_the_cards_cuts(monkeypatch):
    """cfb-data#70: consumers kept a private era cut of 2017 the trainer never
    used, so 2018-2020 scored an era off. The cut now comes from the card."""
    monkeypatch.setattr(mc, "card_era_contract", lambda m: {
        "encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]})
    df = pl.DataFrame({"season": [2005, 2010, 2018, 2024]})
    out = mc.add_era_columns(df, "xpass_model")
    assert out["era"].to_list() == [0, 1, 2, 3]


def test_2018_through_2020_land_in_bucket_2_not_3(monkeypatch):
    """The exact regression from cfb-data#70, pinned."""
    monkeypatch.setattr(mc, "card_era_contract", lambda m: {
        "encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]})
    out = mc.add_era_columns(pl.DataFrame({"season": [2018, 2019, 2020]}), "xpass_model")
    assert out["era"].to_list() == [2, 2, 2]


def test_one_hot_era_produces_all_four_columns(monkeypatch):
    monkeypatch.setattr(mc, "card_era_contract", lambda m: {
        "encoding": "one_hot", "columns": ["era0", "era1", "era2", "era3"],
        "cuts": [2006, 2013, 2020]})
    out = mc.add_era_columns(pl.DataFrame({"season": [2018]}), "fg_model")
    assert out.select(["era0", "era1", "era2", "era3"]).row(0) == (0, 0, 1, 0)


def test_a_model_with_no_era_contract_is_left_untouched(monkeypatch):
    monkeypatch.setattr(mc, "card_era_contract", lambda m: None)
    df = pl.DataFrame({"season": [2018], "yards_to_goal": [30]})
    assert mc.add_era_columns(df, "ep_model").columns == df.columns


def test_an_existing_era_column_is_not_overwritten(monkeypatch):
    """A pbp frame already carries era; recomputing it would fight the pipeline."""
    monkeypatch.setattr(mc, "card_era_contract", lambda m: {
        "encoding": "ordinal", "columns": ["era"], "cuts": [2006, 2013, 2020]})
    df = pl.DataFrame({"season": [2018], "era": [99]})
    assert mc.add_era_columns(df, "xpass_model")["era"].to_list() == [99]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cfb/test_model_calculators.py -q -k era`
Expected: FAIL — `AttributeError: module ... has no attribute 'add_era_columns'`

- [ ] **Step 3: Write minimal implementation**

Append to `model_calculators.py`:

```python
def add_era_columns(df: pl.DataFrame, model: str, season: int | None = None) -> pl.DataFrame:
    """Add the era column(s) ``model`` consumes, using ITS card's cuts.

    The cuts are read from the published contract, never restated here. That is
    the fix for cfb-data#70, where both consumers kept a private copy of the era
    boundary, both drifted to a 2017 cut the trainer never used, and 2018-2020
    scored an era off the models trained with them.

    Args:
        df: Frame carrying a ``season`` column, or any frame when ``season`` is
            given explicitly.
        model: Bundle stem, used to look up the era contract.
        season: Season to use when ``df`` has no ``season`` column -- the
            hand-built-row case.

    Returns:
        ``df`` with the contract's columns added. Unchanged when the model
        declares no era contract, or when the columns are already present.

    Example:
        Quick start::

            add_era_columns(pl.DataFrame({"season": [2018]}), "xpass_model")
    """
    contract = card_era_contract(model)
    if not contract:
        return df
    columns = contract["columns"]
    if all(c in df.columns for c in columns):
        return df
    lo, mid, hi = contract["cuts"]
    if season is not None:
        season_expr = pl.lit(season, dtype=pl.Int64)
    elif "season" in df.columns:
        season_expr = pl.col("season").cast(pl.Int64)
    else:
        raise ValueError(
            f"{model} needs an era column; supply a 'season' column or the season= argument"
        )
    bucket = (
        pl.when(season_expr <= lo).then(0)
        .when(season_expr <= mid).then(1)
        .when(season_expr <= hi).then(2)
        .otherwise(3)
    )
    if contract["encoding"] == "ordinal":
        return df.with_columns(bucket.cast(pl.Int32).alias(columns[0]))
    return df.with_columns(
        [(bucket == i).cast(pl.Int32).alias(col) for i, col in enumerate(columns)]
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/cfb/test_model_calculators.py -q`
Expected: PASS (9 passed)

- [ ] **Step 5: Lint, typecheck, lockfile**

Run: `uv run ruff check sportsdataverse/cfb/model_calculators.py && uv run mypy sportsdataverse/cfb/model_calculators.py && git status --short uv.lock`

- [ ] **Step 6: Commit**

```bash
git add sportsdataverse/cfb/model_calculators.py tests/cfb/test_model_calculators.py
git commit -m "feat(cfb): derive era columns from the model's own card

The cuts come from the published era_contract, never restated here. That is the
structural fix for cfb-data#70: both consumers kept a private copy of the era
boundary, both drifted to a 2017 cut the trainer never used, and 2018-2020
scored an era off the models trained with them. A test pins that exact case.

An era column already present is left alone, so a pbp frame that carries one
does not get it recomputed underneath the pipeline."
```

---

### Task 4: The ten public calculators

**Files:**
- Modify: `sportsdataverse/cfb/model_calculators.py`
- Modify: `sportsdataverse/parsed/cfb.py`
- Test: `tests/cfb/test_model_calculators.py` (append)

**Interfaces:**
- Consumes: `predict_from_card`, `add_era_columns`, and the boosters already loaded in `sportsdataverse/cfb/cfb_pbp.py`.
- Produces: `calculate_expected_points`, `calculate_win_probability`, `calculate_epa`, `calculate_wpa`, `calculate_field_goal_probability`, `calculate_completion_probability`, `calculate_xpass`, `calculate_two_point_probability`, `calculate_fourth_down`, `calculate_qbr` — each `(df, *, season=None, return_as_pandas=False)`.

- [ ] **Step 1: Write the failing test**

```python
def test_xpass_returns_the_frame_plus_one_probability_column():
    """The contract: return the caller's frame with model output appended, so a
    hand-built row and a pbp frame behave identically."""
    df = pl.DataFrame({
        "season": [2024], "down": [3.0], "distance": [8.0],
        "yards_to_goal": [55.0], "pos_score_diff": [-4.0],
        "TimeSecsRem": [900.0], "period": [3.0],
    })
    out = mc.calculate_xpass(df)
    assert out.height == 1
    assert "xpass" in out.columns
    assert 0.0 <= out["xpass"][0] <= 1.0
    for c in df.columns:
        assert c in out.columns, f"calculator dropped the caller's column {c}"


def test_a_hand_built_row_scores_without_any_pbp_machinery():
    """The hypothetical case: someone types a situation and asks the model."""
    out = mc.calculate_field_goal_probability(
        pl.DataFrame({"season": [2024], "yards_to_goal": [25.0]})
    )
    assert 0.0 <= out["fg_prob"][0] <= 1.0


def test_a_missing_column_names_what_is_absent():
    with pytest.raises(ValueError, match="yards_to_goal"):
        mc.calculate_field_goal_probability(pl.DataFrame({"season": [2024]}))


def test_return_as_pandas_is_honoured():
    import pandas as pd

    out = mc.calculate_field_goal_probability(
        pl.DataFrame({"season": [2024], "yards_to_goal": [25.0]}), return_as_pandas=True
    )
    assert isinstance(out, pd.DataFrame)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cfb/test_model_calculators.py -q -k "xpass or hand_built"`
Expected: FAIL — `AttributeError: module ... has no attribute 'calculate_xpass'`

- [ ] **Step 3: Write minimal implementation**

Append to `model_calculators.py`. Import the already-loaded boosters from `sportsdataverse.cfb.cfb_pbp` **inside each function**, not at module top: `cfb_pbp` is a heavy import and a top-level one risks a cycle.

Write one private helper and ten thin wrappers over it:

```python
def _calculate(df, model, out_col, *, season=None, return_as_pandas=False, transform=None):
    """Shared body: derive era, validate against the card, predict, append.

    Args:
        df: Caller's frame.
        model: Bundle stem.
        out_col: Name for the appended output column.
        season: Season for era derivation when ``df`` carries none.
        return_as_pandas: Return a pandas frame instead of polars.
        transform: Optional callable mapping raw predictions to the output
            column (used by the multiclass EP model).

    Returns:
        The caller's frame with ``out_col`` appended. Every input column is
        preserved -- a calculator that dropped columns would make chaining
        two of them lossy.
    """
    prepared = add_era_columns(df, model, season=season)
    raw = predict_from_card(prepared, model, _booster_for(model))
    values = transform(raw) if transform else raw
    out = prepared.with_columns(pl.Series(out_col, values))
    return out.to_pandas() if return_as_pandas else out
```

There is **no** `BOOSTERS` mapping in the package -- verified. The boosters are
module-level names spread across three modules, and several are loaded lazily. Write an
explicit resolver, and import inside it because `cfb_pbp` is a heavy import and a
top-level import risks a cycle:

```python
def _booster_for(model: str):
    """Resolve a card stem to its loaded booster.

    The boosters are module-level names across three modules rather than one
    mapping, and some are lazily loaded, so this is the single place that knows
    where each lives. Imports are function-local: `cfb_pbp` is heavy and a
    top-level import risks a cycle.

    Args:
        model: Bundle stem, e.g. ``"wp_spread"``.

    Returns:
        The loaded ``xgb.Booster``.

    Raises:
        KeyError: When the stem has no known booster -- better than scoring
            against the wrong model.
    """
    from sportsdataverse.cfb import cfb_pbp

    if model in {"ep_model", "wp_spread", "wp_naive", "qbr_model",
                 "cfb_cp_model", "xpass_model"}:
        return {
            "ep_model": cfb_pbp.ep_model,
            "wp_spread": cfb_pbp.wp_model,        # wp_model IS wp_spread -- it loads
            "wp_naive": cfb_pbp.wp_naive_model,   # from wp_spread_file (cfb_pbp.py:245)
            "qbr_model": cfb_pbp.qbr_model,
            "cfb_cp_model": cfb_pbp.cp_model,
            "xpass_model": cfb_pbp.xpass_model,
        }[model]
    if model == "two_pt_model":
        from sportsdataverse.cfb import cfb_two_point

        return cfb_two_point.two_pt_model
    if model in {"fg_model", "fd_model"}:
        from sportsdataverse.cfb import cfb_fourth_down

        # Both are lazily loaded in cfb_fourth_down; read that module to find the
        # accessor it exposes rather than reaching for a module global that may
        # not be populated until first use.
        return cfb_fourth_down._load_model(model)  # confirm the real accessor name
    raise KeyError(f"no booster registered for {model!r}")
```

**Verify the fg/fd/two_pt accessors against the real modules before writing this** --
`cfb_fourth_down.py` and `cfb_two_point.py` load lazily and the accessor name above is a
placeholder to confirm, not a fact. Record what you find in your report.

Then each public function is a documented wrapper, e.g.:

```python
def calculate_xpass(df, *, season=None, return_as_pandas=False):
    """Expected pass probability for each row.

    Mirrors ``sportsdataverse.nfl.calculate_xpass()``. Rows may come from a pbp
    frame or be typed by hand; only the card's declared columns are required.

    Args:
        df: Frame with ``down``, ``distance``, ``yards_to_goal``,
            ``pos_score_diff``, ``TimeSecsRem``, ``period``, and either a
            ``season`` column or the ``season`` argument for the era feature.
        season: Season used to derive ``era`` when ``df`` has no season column.
        return_as_pandas: Return pandas instead of polars.

    Returns:
        ``df`` with an ``xpass`` column appended.

    Raises:
        ValueError: When a declared feature is missing; the message names each
            absent column and the model that wanted it.

    Example:
        Quick start::

            calculate_xpass(pl.DataFrame({"season":[2024],"down":[3.0],
                "distance":[8.0],"yards_to_goal":[55.0],"pos_score_diff":[-4.0],
                "TimeSecsRem":[900.0],"period":[3.0]}))
    """
    return _calculate(df, "xpass_model", "xpass", season=season,
                      return_as_pandas=return_as_pandas)
```

Write the remaining nine the same way. `calculate_expected_points` needs a `transform` that maps the multiclass `multi:softprob` output onto the class order in `MANIFEST.json`'s `ep_class_contract.class_order` and produces `ep` — read that contract, do not restate the order. `calculate_win_probability` selects `wp_spread` when the frame carries a spread column and `wp_naive` otherwise, and must document which it chose.

Add all ten to `sportsdataverse/parsed/cfb.py` following the existing re-export pattern there.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/cfb/test_model_calculators.py -q`
Expected: PASS (13 passed)

- [ ] **Step 5: Regenerate codegen and verify no drift**

Run: `uv run python tools/codegen/generate.py && uv run python tools/codegen/generate.py --check`
Expected: `all generated files current`. New public functions must land in the parsed modules and reference docs.

- [ ] **Step 6: Lint, typecheck, lockfile**

Run: `uv run ruff check sportsdataverse/ tests/cfb/ && uv run mypy sportsdataverse/cfb/model_calculators.py && git status --short uv.lock`

- [ ] **Step 7: Commit**

```bash
git add sportsdataverse/cfb/model_calculators.py sportsdataverse/parsed/cfb.py tests/cfb/test_model_calculators.py
git commit -m "feat(cfb): ten user-facing model calculators

Hand a frame to calculate_expected_points() and get model output back, whether
the rows came from a real game or were typed by hand -- the surface
sportsdataverse.nfl already provides and CFB did not, despite shipping nine
trained models.

Every calculator preserves the caller's columns and appends its output, so
chaining two of them is lossless. calculate_win_probability picks wp_spread
when a spread is present and wp_naive otherwise, mirroring the pipeline."
```

---

### Task 5: Delete the duplicated feature lists

**Files:**
- Modify: `sportsdataverse/cfb/cfb_fourth_down.py` (`EP_FEATURES`, `WP_SPREAD_FEATURES`, `FD_FEATURES`)
- Modify: `sportsdataverse/cfb/cfb_two_point.py` (`TWO_PT_FEATURES`)
- Modify: `sportsdataverse/cfb/cfb_pbp.py` (`CP_FEATURES`, `XPASS_FEATURES`)
- Modify: `sportsdataverse/cfb/models/*.card.json` — refresh from the release if stale
- Test: `tests/cfb/test_model_calculators.py` (append)

**Interfaces:**
- Consumes: `card_features` from Task 1.

- [ ] **Step 1: Write the failing test**

```python
import pytest

CARD_BACKED = [
    ("ep_model", "sportsdataverse.cfb.cfb_fourth_down", "EP_FEATURES"),
    ("wp_spread", "sportsdataverse.cfb.cfb_fourth_down", "WP_SPREAD_FEATURES"),
    ("fd_model", "sportsdataverse.cfb.cfb_fourth_down", "FD_FEATURES"),
    ("two_pt_model", "sportsdataverse.cfb.cfb_two_point", "TWO_PT_FEATURES"),
    ("cfb_cp_model", "sportsdataverse.cfb.cfb_pbp", "CP_FEATURES"),
    ("xpass_model", "sportsdataverse.cfb.cfb_pbp", "XPASS_FEATURES"),
]


@pytest.mark.parametrize("model,module,const", CARD_BACKED)
def test_no_feature_list_drifts_from_its_card(model, module, const):
    """Every hardcoded feature list must equal its card, in order.

    These six constants restate what the cards publish. Left unpinned they drift
    -- which is precisely how cfb-data#70 happened one layer down, with the era
    cuts. This test fails the moment a retrain changes a model's features."""
    import importlib

    from sportsdataverse.cfb.model_cards import card_features

    local = getattr(importlib.import_module(module), const)
    assert list(local) == card_features(model), f"{const} drifted from {model}'s card"
```

- [ ] **Step 2: Run test to verify it fails or reveals stale cards**

Run: `uv run pytest tests/cfb/test_model_calculators.py -q -k drifts`
Expected: This may FAIL — either because a constant genuinely drifted, or because the packaged `sportsdataverse/cfb/models/*.card.json` copies predate cfb-data#79. Diagnose which before changing code:

```bash
uv run python -c "
import json,urllib.request
from pathlib import Path
for m in ('fg_model','fd_model','two_pt_model','xpass_model','cfb_cp_model'):
    live=json.load(urllib.request.urlopen('https://github.com/sportsdataverse/sportsdataverse-data/releases/download/cfb_model_artifacts/%s.card.json'%m))
    p=Path('sportsdataverse/cfb/models/%s.card.json'%m)
    local=json.loads(p.read_text()) if p.exists() else None
    print(m, 'local era:', (local or {}).get('era_contract') is not None if local else 'MISSING',
          '| live era:', live.get('era_contract') is not None)"
```

- [ ] **Step 3: Refresh the packaged cards, then make each constant read its card**

If the packaged cards are stale or missing, download the nine `*.card.json` from the release into `sportsdataverse/cfb/models/`. Then replace each constant with a card read, e.g. in `cfb_fourth_down.py`:

```python
from sportsdataverse.cfb.model_cards import card_features

#: Read from the model's published card rather than restated. A retrain that
#: changes the feature set now propagates instead of silently disagreeing --
#: the failure mode of cfb-data#70, one layer down.
EP_FEATURES = card_features("ep_model")
WP_SPREAD_FEATURES = card_features("wp_spread")
FD_FEATURES = card_features("fd_model")
```

Keep the constant NAMES — they are imported elsewhere in the package and renaming them is out of scope.

- [ ] **Step 4: Run the full CFB suite to prove nothing regressed**

Run: `uv run pytest tests/cfb/ -q`
Expected: PASS. These constants feed the live scoring path, so any behavior change surfaces here. If a test fails, the constant did NOT match its card — report which and stop; that is a real finding, not a test to adjust.

- [ ] **Step 5: Regenerate codegen, lint, typecheck, lockfile**

Run: `uv run python tools/codegen/generate.py --check && uv run ruff check sportsdataverse/ && git status --short uv.lock`

- [ ] **Step 6: Commit**

```bash
git add sportsdataverse/cfb/ tests/cfb/test_model_calculators.py
git commit -m "refactor(cfb): read feature lists from the cards instead of restating them

Six constants restated what the model cards publish. Left unpinned they drift,
which is exactly how cfb-data#70 happened one layer down: both consumers kept a
private copy of the era cuts, both drifted to a 2017 boundary the trainer never
used, and 2018-2020 scored an era off the models trained with them.

Each now reads its card, and a parametrised test fails the moment any of them
disagrees with the published contract."
```

---

## Self-Review

**Spec coverage.** Task 1 implements the spec's "contract source"; Task 2 its `predict_from_card` layer and error-handling section; Task 3 the era portion of feature derivation; Task 4 the ten public functions and the naming decision; Task 5 removes the duplication the spec identifies as the motivating defect. The spec's cross-language parity fixture belongs to Plan 3 and is deliberately absent here.

**Placeholder scan.** No TBD/TODO. Task 4 Step 3 describes nine wrappers by showing one complete worked example plus the two that differ (`calculate_expected_points`'s class-order transform, `calculate_win_probability`'s model selection) rather than repeating near-identical code ten times; the shared `_calculate` helper carries the actual logic and is given in full.

**Type consistency.** `card_features(model) -> list[str]` and `card_era_contract(model) -> dict | None` are defined in Task 1 and used unchanged in Tasks 2, 3 and 5. `predict_from_card(df, model, booster) -> np.ndarray` is defined in Task 2 and consumed by Task 4's `_calculate`. `add_era_columns(df, model, season=None) -> pl.DataFrame` is defined in Task 3 and consumed by the same helper.

**Resolved before dispatch.** An earlier draft assumed a `BOOSTERS` mapping in `cfb_pbp`. There is none: the boosters are module-level names across `cfb_pbp.py` (`ep_model`, `wp_model` = wp_spread, `wp_naive_model`, `qbr_model`, `cp_model`, `xpass_model`), `cfb_two_point.py` and `cfb_fourth_down.py`, and several load lazily. Task 4 now specifies an explicit `_booster_for()` resolver, with the fg/fd/two_pt accessors flagged for the implementer to confirm against those modules.
