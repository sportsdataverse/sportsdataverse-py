"""Phase 2 -- nba_tracking_pass_value (expected assists / passer value)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_tracking_value import nba_tracking_pass_value

_AST_SCHEMA = [
    "season",
    "player_id",
    "player_name",
    "team_id",
    "position_bucket",
    "gp",
    "min",
    "ast",
    "passes",
    "ast_baseline_rate",
    "ast_expected",
    "ast_oe",
    "ast_oe_per_36",
    "ast_pts_created",
    "league_id",
]


def _fake_passing_payload():
    headers = ["PLAYER_ID", "PLAYER_NAME", "TEAM_ID", "GP", "MIN", "PASSES_MADE", "AST", "AST_PTS_CREATED"]
    rows = [
        [1630169, "A", 1610612754, 50, 1800.0, 400.0, 100.0, 220.0],
        [2544, "B", 1610612747, 50, 1800.0, 400.0, 60.0, 140.0],
    ]
    return {"resultSets": [{"name": "LeagueDashPtStats", "headers": headers, "rowSet": rows}]}


def test_ast_oe_schema_and_math():
    calls = []

    def fake(**kw):
        calls.append(kw)
        return _fake_passing_payload()

    out = nba_tracking_pass_value(2024, by_position=False, _get_fn=fake)
    assert out.columns == _AST_SCHEMA
    assert out.schema["player_id"] == pl.Utf8
    assert len(calls) == 1  # exactly one fetch by default (no potential-assist enrichment)

    rows = {r["player_id"]: r for r in out.iter_rows(named=True)}
    # equal passes (400 each) -> baseline rate = (100+60)/(400+400) = 0.2
    assert abs(rows["1630169"]["ast_baseline_rate"] - 0.2) < 1e-9
    assert abs(rows["1630169"]["ast_expected"] - 80.0) < 1e-9
    assert abs(rows["1630169"]["ast_oe"] - 20.0) < 1e-9
    assert abs(rows["2544"]["ast_oe"] - (-20.0)) < 1e-9
    # ast_pts_created passed through as-is, not recomputed
    assert rows["1630169"]["ast_pts_created"] == 220.0


def test_ast_oe_potential_assists_enrichment_swaps_denom():
    def fake(**kw):
        return _fake_passing_payload()

    def fake_pass(**kw):
        pid = kw["player_id"]
        potential = 500.0 if pid == "1630169" else 500.0
        headers = ["PLAYER_ID", "POTENTIAL_AST"]
        return {"resultSets": [{"name": "PassesMade", "headers": headers, "rowSet": [[int(pid), potential]]}]}

    out = nba_tracking_pass_value(
        2024,
        by_position=False,
        fetch_potential_assists=True,
        max_players=2,
        _get_fn=fake,
        _pass_get_fn=fake_pass,
    )
    from sportsdataverse.nba.nba_tracking_value_constants import residual_sums_to_zero

    # equal potential_assists (500 each) -> still sums to zero within the bucket
    assert residual_sums_to_zero(out, "ast_oe", []) is True
    # original passes column untouched by the enrichment
    assert out["passes"].to_list() == [400.0, 400.0]


def test_ast_oe_empty_is_zero_row_schema():
    out = nba_tracking_pass_value(2024, _get_fn=lambda **kw: {"resultSets": []})
    assert out.height == 0 and out.columns == _AST_SCHEMA
