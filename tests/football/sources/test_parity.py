"""Parity harness: a game against itself is perfect; a perturbed copy reports exactly the perturbations."""

from __future__ import annotations

import math
from types import SimpleNamespace

import polars as pl
import pytest

from sportsdataverse.football.sources.parity import (
    GOP_BOX_SECTIONS,
    GOP_HARD_COLUMNS,
    NUMERIC_PARITY_COLUMNS,
    STATE_KEY,
    _compare_plays,
    _compare_processed,
)


def test_gop_hard_columns_are_all_produced_by_the_espn_path(nfl_processed):
    assert len(GOP_HARD_COLUMNS) == 60
    assert [c for c in GOP_HARD_COLUMNS if c not in nfl_processed.plays_frame.columns] == []
    assert set(GOP_BOX_SECTIONS) <= set(nfl_processed.game["advBoxScore"])


def test_self_parity_is_perfect(nfl_processed):
    r = _compare_processed(nfl_processed, nfl_processed)
    n = nfl_processed.plays_frame.height
    assert (r.n_reference, r.n_candidate, r.n_paired) == (n, n, n)
    assert r.missing_columns == []
    assert set(r.agreement) == set(GOP_HARD_COLUMNS) - {"id"}
    assert all(v == 1.0 for v in r.agreement.values()), {k: v for k, v in r.agreement.items() if v < 1}
    assert set(r.correlation) == set(NUMERIC_PARITY_COLUMNS)
    assert all(math.isclose(v, 1.0, abs_tol=1e-9) for v in r.correlation.values())
    assert all(v == 0.0 for v in r.mean_abs_diff.values())
    for s in GOP_BOX_SECTIONS:
        assert r.box_rows[s]["equal_rows"] == r.box_rows[s]["n_reference"] == r.box_rows[s]["n_candidate"]
    gates = r.gates()
    assert gates["paired_share"] == 1.0 and r.check(gates) == []


@pytest.fixture
def perturbed(nfl_processed):
    frame = nfl_processed.plays_frame
    n = frame.height
    idx = pl.int_range(0, n, eager=True)
    cand = (
        frame.with_columns(
            EPA=pl.when(idx < 5).then(pl.col("EPA") + 0.5).otherwise(pl.col("EPA")),
            **{"type.text": pl.when(idx.is_in([20, 21, 22])).then(pl.lit("Perturbed")).otherwise(pl.col("type.text"))},
        )
        .drop("wp_after")
        .sample(fraction=1.0, shuffle=True, seed=7)  # row order must not matter: pairing is on id
    )
    box = dict(nfl_processed.game["advBoxScore"])
    box["rush"] = list(box["rush"])[1:]
    return SimpleNamespace(plays_frame=cand, game={"advBoxScore": box}), n


def test_perturbed_copy_reports_exact_deltas(nfl_processed, perturbed):
    cand, n = perturbed
    r = _compare_processed(nfl_processed, cand)
    assert r.n_paired == n and r.missing_columns == ["wp_after"]
    assert math.isclose(r.agreement["EPA"], 1 - 5 / n)
    assert math.isclose(r.agreement["type.text"], 1 - 3 / n)
    assert math.isclose(r.agreement["EP_start"], 1.0)
    assert 0.99 < r.correlation["EPA"] < 1.0 and "wp_after" not in r.correlation
    assert math.isclose(r.mean_abs_diff["EPA"], 0.5 * 5 / n, rel_tol=1e-6)
    n_rush = len(nfl_processed.game["advBoxScore"]["rush"])
    assert r.box_rows["rush"] == {"n_reference": n_rush, "n_candidate": n_rush - 1, "equal_rows": n_rush - 1}
    assert r.box_rows["pass"]["equal_rows"] == r.box_rows["pass"]["n_reference"]

    pinned = _compare_processed(nfl_processed, nfl_processed).gates() | {"required_columns": ["wp_after"]}
    fails = r.check(pinned)
    assert sorted(f.split(" ")[0] for f in fails) == sorted(
        [
            "agreement.EPA",
            "agreement.type.text",
            "agreement.wp_after",
            "correlation.EPA",
            "correlation.wp_after",
            "box_equal_share.rush",
            "column",
        ]
    )


def test_gates_are_floors_never_ceilings(nfl_processed):
    r = _compare_processed(nfl_processed, nfl_processed)
    assert r.check({"agreement": {"EPA": 0.9}, "correlation": {"EPA": 0.99}, "paired_share": 0.5}) == []
    assert r.check({"agreement": {"EPA": 1.0001}}) == ["agreement.EPA 1.0 < 1.0001"]
    assert r.check({"agreement": {"not_a_column": 0.1}}) == ["agreement.not_a_column None < 0.1"]


def test_state_key_pairing_for_sources_without_espn_ids(nfl_processed):
    frame = nfl_processed.plays_frame
    cand = frame.drop("id").with_row_index("id", offset=100_000)  # foreign ids: only game state can pair
    r = _compare_plays(frame, cand, key=STATE_KEY)
    unique_states = frame.select(STATE_KEY).unique().height
    assert r.n_paired == unique_states <= frame.height
    assert all(v == 1.0 for k, v in r.agreement.items()), {k: v for k, v in r.agreement.items() if v < 1}
    assert "id" not in r.agreement


def test_empty_pairing_is_reported_not_raised():
    a = pl.DataFrame({"id": [1, 2], "EPA": [0.1, 0.2]})
    b = pl.DataFrame({"id": [3], "EPA": [0.3]})
    r = _compare_plays(a, b, columns=("EPA",))
    assert r.n_paired == 0 and r.agreement == {} and r.gates()["paired_share"] == 0.0


def test_a_nan_gate_is_a_failure_not_a_pass():
    """``pl.corr`` is NaN on a zero-variance column; NaN < floor is False in Python.

    A source whose WP model never ran ships a constant ``wp_before``, and a bare
    ``got < floor`` would let that pin itself as a passing gate forever.
    """
    const = pl.DataFrame({"id": [1, 2, 3], "EPA": [0.5, 0.5, 0.5]})
    r = _compare_plays(const, const, columns=("EPA",), numeric=("EPA",))
    assert math.isnan(r.correlation["EPA"])
    assert r.check({"correlation": {"EPA": 0.99}}) == ["correlation.EPA nan < 0.99"]


def test_pairing_key_dtype_and_presence_are_asserted():
    """Float64 "1.0" never joins Int64 "1": a silent zero-row pairing would pin itself as the gate."""
    ref = pl.DataFrame({"id": [1, 2], "EPA": [0.1, 0.2]})
    with pytest.raises(ValueError, match="dtype differs"):
        _compare_plays(ref, pl.DataFrame({"id": [1.0, 2.0], "EPA": [0.1, 0.2]}), columns=("EPA",))
    with pytest.raises(ValueError, match="absent from the candidate frame"):
        _compare_plays(ref, pl.DataFrame({"play_id": [1, 2], "EPA": [0.1, 0.2]}), columns=("EPA",))


def test_no_shared_compared_column_is_reported_not_raised():
    """A candidate carrying none of the compared columns reports them missing, it does not crash."""
    ref = pl.DataFrame({"id": [1, 2], "EPA": [0.1, 0.2]})
    r = _compare_plays(ref, pl.DataFrame({"id": [1, 2], "other": [1, 2]}), columns=("EPA",))
    assert r.n_paired == 2 and r.missing_columns == ["EPA"] and r.agreement == {}
    assert r.check({"required_columns": ["EPA"]}) == ["column missing: EPA"]
