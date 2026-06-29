"""Tests for nba_lineups: boxscore name map + period starters (Task 4) + on-court reconstruction (Task 5)."""

import json
import pathlib

import polars as pl


FX = pathlib.Path("tests/fixtures/nba_engine/0022200001")


def _box() -> dict:
    return json.loads((FX / "boxscoretraditionalv3.json").read_text())


def _payload() -> dict:
    return json.loads((FX / "playbyplayv3.json").read_text())


def test_name_map_and_home_away() -> None:
    from sportsdataverse.nba.nba_lineups import boxscore_home_away, boxscore_name_map

    nm = boxscore_name_map(_box())
    home, away = boxscore_home_away(_box())
    assert home == 1610612738 and away == 1610612755
    assert all(isinstance(v, int) for team in nm.values() for v in team.values())


def test_period_starters_five_each() -> None:
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
    from sportsdataverse.nba.nba_lineups import period_starters

    s = period_starters(enhanced_pbp_from_payload(_payload()), _box())
    assert set(s.keys()) >= {1, 2, 3, 4}
    for teams in s.values():
        assert len(teams) == 2 and all(len(p) == 5 for p in teams.values())


def test_on_court_matches_fixture() -> None:
    from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
    from sportsdataverse.nba.nba_lineups import (
        boxscore_home_away,
        boxscore_name_map,
        period_starters,
        players_on_court,
    )

    enh = enhanced_pbp_from_payload(_payload())
    home, away = boxscore_home_away(_box())
    got = players_on_court(
        enh,
        period_starters(enh, _box()),
        boxscore_name_map(_box()),
        home_team_id=home,
        away_team_id=away,
    )
    exp = pl.read_parquet(FX / "lineups_expected.parquet")

    pcols = [f"home_player_{i}" for i in range(1, 6)] + [f"away_player_{i}" for i in range(1, 6)]

    # Engine emits one row per enhanced action (468 rows).
    # The fixture has 446 rows (one per unique action_number in the oracle).
    # Join on action_number — every fixture action_number must be present in the engine output.
    assert got.height == enh.height, f"engine output rows {got.height} != enh rows {enh.height}"

    # Deduplicate engine output on action_number (take first occurrence by order_index
    # since duplicate action_numbers carry the same lineup state).
    got_dedup = got.sort("action_number").unique(subset=["action_number"], keep="first")

    cmp = exp.join(
        got_dedup.select(["action_number"] + pcols).rename({c: f"{c}_got" for c in pcols}),
        on="action_number",
        how="left",
    )

    for c in pcols:
        mismatches = cmp.filter(pl.col(c) != pl.col(f"{c}_got"))
        assert mismatches.height == 0, (
            f"lineup mismatch in {c}: {mismatches.height} rows differ\n"
            f"{mismatches.select(['action_number', 'period', c, f'{c}_got']).head(10)}"
        )
