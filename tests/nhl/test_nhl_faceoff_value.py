from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_faceoff_value import extract_faceoffs, nhl_faceoff_value


def _row(type_desc_key: str, **overrides: object) -> dict:
    base = {
        "game_id": "G1",
        "season": 2024,
        "event_idx": 0,
        "period": 1,
        "time_in_period": "10:00",
        "type_desc_key": type_desc_key,
        "event_owner_team_id": "10",
        "zone_code": "N",
        "x_coord": 0.0,
        "y_coord": 0.0,
        "situation_code": "1551",
        "home_team_id": "10",
        "home_team_defending_side": "left",
        "winning_player_id": None,
        "losing_player_id": None,
        "scoring_player_id": None,
        "assist1_player_id": None,
        "assist2_player_id": None,
        "shooting_player_id": None,
        "committed_player_id": None,
        "drawn_player_id": None,
        "penalty_type_code": None,
        "shot_type": None,
    }
    base.update(overrides)
    return base


def _fo(winner: str, loser: str, zone: str, sit: str = "1551") -> dict:
    return _row(
        "faceoff",
        zone_code=zone,
        situation_code=sit,
        winning_player_id=winner,
        losing_player_id=loser,
    )


def test_extract_faceoffs_basic() -> None:
    pbp = pl.DataFrame([_fo("A", "B", "O"), _row("shot-on-goal")])
    out = extract_faceoffs(pbp)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["winner_player_id"] == "A"
    assert row["loser_player_id"] == "B"
    assert row["strength_state"] == "even"


def test_extract_faceoffs_empty() -> None:
    empty = extract_faceoffs(pl.DataFrame(schema={"type_desc_key": pl.Utf8}))
    assert empty.height == 0
    assert "winner_player_id" in empty.columns


def test_faceoff_value_dominant_center() -> None:
    rows = [_fo("A", "B", "O") for _ in range(8)] + [_fo("B", "A", "O") for _ in range(2)]
    pbp = pl.DataFrame(rows)
    out = nhl_faceoff_value(pbp).sort("player_id")
    a = out.filter(pl.col("player_id") == "A").row(0, named=True)
    assert a["faceoffs_taken"] == 10 and a["faceoffs_won"] == 8
    assert abs(a["fo_win_pct"] - 0.8) < 1e-9
    assert a["fo_win_pct_above_exp"] > 0  # beats the 50% context baseline
    assert a["faceoff_value"] > 0


def test_empty_pbp_returns_schema() -> None:
    empty = nhl_faceoff_value(pl.DataFrame(schema={"type_desc_key": pl.Utf8}))
    assert empty.height == 0
    assert "faceoff_value" in empty.columns


def test_context_fit_reproduces_real_cell_win_rates() -> None:
    """C-L30: the four tests above passed with the fit skipped (`if True:` -> constant 0.5).

    On the committed real 2024 api-web slice (`tests/fixtures/nhl_microstat/pbp_2024_slice.parquet`,
    7,016 faceoffs; README there) the fitted context logistic must reproduce each cell's
    empirical taker win rate. Observed 2026-10-11: power play 0.5527 (pred 0.5523), penalty
    kill 0.4473 (0.4472), home/away by zone within 0.0006 of empirical. A constant 0.5 misses
    the power-play cell by 0.053.
    """
    from pathlib import Path

    from sportsdataverse.nhl.nhl_faceoff_value import _taker_perspective_rows, fit_faceoff_context

    pbp = pl.read_parquet(Path(__file__).resolve().parents[1] / "fixtures" / "nhl_microstat" / "pbp_2024_slice.parquet")
    faceoffs = extract_faceoffs(pbp)
    assert faceoffs.height == 7016
    rows = _taker_perspective_rows(faceoffs)
    rows = rows.with_columns(fit_faceoff_context(faceoffs).predict(rows))
    for keys in (["strength_state"], ["zone_code", "is_home"]):
        cells = rows.group_by(keys).agg(
            pl.col("won").mean().alias("empirical"), pl.col("expected_win").mean().alias("predicted")
        )
        worst = (cells["empirical"] - cells["predicted"]).abs().max()
        assert worst <= 0.002, f"{keys}: worst cell gap {worst:.4f}\n{cells}"
    pp = rows.filter(pl.col("strength_state") == "pp")["expected_win"].mean()
    assert 0.545 <= pp <= 0.56, pp
    print("NHL_FACEOFF_FIT_OK", round(pp, 4))
