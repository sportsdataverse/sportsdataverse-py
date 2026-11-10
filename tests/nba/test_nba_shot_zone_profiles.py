"""BB-D35: NBA / WNBA season shot profile by zone on committed real captures.

Fixtures (``tests/fixtures/bb_shot_zones``, README there; none synthetic): the first 120
regular-season games of the 2024-25 NBA ``nba_stats_shots`` release, and the first 40 of the 2010
and 2025 WNBA ``wnba_stats_shots`` releases (2010 is a hoop-centre placeholder season).

The gates here: every attempt lands in exactly one zone or is unlocated, so the four zone counts
plus ``unlocated_fga`` add up to ``fga``, for players, teams and opponents alike; and the corner
threes sit where the court puts them (beyond the line, in the baseline band) on each era's court.

The ``bb-shot-zones`` rows of ``tests/mutants/registry.yaml`` go red here.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.shot_zone_profiles import (
    SHOT_ROWS_SCHEMA,
    ZONES,
    player_shot_zones,
    team_shot_zones,
    zone_columns,
)
from sportsdataverse.nba import nba_shot_zone_profiles as nsz

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_shot_zones"
CASES = [
    ("nba_stats_shots_2025", "00", "nba"),
    ("wnba_stats_shots_2010", "10", "wnba"),
    ("wnba_stats_shots_2025", "10", "wnba"),
]


def _shots(name: str) -> pl.DataFrame:
    """A committed shots capture."""
    return pl.read_parquet(FIX / f"{name}.parquet")


def _rows(name: str, league_id: str) -> pl.DataFrame:
    """A committed shots capture mapped onto the shot rows."""
    return nsz.nba_shot_zone_rows(_shots(name), league_id=league_id)


def _zone_sum(prefix: str = "") -> pl.Expr:
    """The four zone attempt counts plus the unlocated ones."""
    return sum((pl.col(f"{prefix}{z}_fga") for z in ZONES), pl.col(f"{prefix}unlocated_fga"))


@pytest.mark.parametrize(("name", "league_id", "level"), CASES)
def test_zone_attempts_add_up_to_fga(name: str, league_id: str, level: str) -> None:
    """Every attempt lands in one zone or is unlocated, for players, teams and opponents."""
    rows = _rows(name, league_id)
    assert rows.schema == pl.Schema(SHOT_ROWS_SCHEMA)
    assert rows.height == _shots(name).height  # every fixture row is a regular-season FGA
    players = player_shot_zones(rows, level=level)
    teams = team_shot_zones(rows, level=level)
    assert players.select((_zone_sum() == pl.col("fga")).all()).item()
    assert teams.select((_zone_sum() == pl.col("fga")).all() & (_zone_sum("opp_") == pl.col("opp_fga")).all()).item()
    # each attempt is counted once for its shooter's team and once against the other team
    assert teams["fga"].sum() == teams["opp_fga"].sum() == rows.filter(pl.col("team_id").is_not_null()).height
    assert players["fga"].sum() == rows.height
    # a row's four zone shares add up to 1
    assert teams.select(pl.sum_horizontal(pl.col(f"{z}_freq") for z in ZONES).sub(1).abs().max()).item() < 1e-12


def test_corner_three_geometry_nba() -> None:
    """2024-25 NBA: every corner three is a three in the band below the break, at the 22 ft corner."""
    s = _shots("nba_stats_shots_2025")
    r = _rows("nba_stats_shots_2025", "00").with_columns(
        s.select("shot_value", "shot_distance", "x_legacy", "y_legacy")
    )
    corner = r.filter(pl.col("zone") == "corner_3")
    assert corner.height == 2223  # observed on the fixture
    assert corner.select(
        (pl.col("shot_value") == 3).all(),
        (pl.col("y_legacy") <= 87.5).all(),
        (pl.col("x_legacy").abs() >= 220).all(),
        (pl.col("shot_distance") >= 22).all(),
    ).row(0) == (True, True, True, True)
    threes = r.filter(pl.col("shot_value") == 3)
    assert threes["zone"].is_in(["corner_3", "above_break_3"]).all()
    assert threes.filter(pl.col("zone") == "above_break_3")["y_legacy"].min() > 87.5
    twos = r.filter(pl.col("shot_value") == 2)
    assert twos["zone"].is_in(["rim", "mid"]).all()
    assert twos.filter(pl.col("zone") == "rim")["shot_distance"].max() < 4


@pytest.mark.parametrize(("name", "line"), [("wnba_stats_shots_2010", 205), ("wnba_stats_shots_2025", 220)])
def test_corner_three_geometry_wnba(name: str, line: int) -> None:
    """WNBA: the band test alone; a corner three is still beyond that era's line (20 ft 6.25 in in
    2010, 22 ft from 2013), and the 2010 corner (about 20.5 ft) is mostly inside 22 ft laterally."""
    s = _shots(name)
    r = _rows(name, "10").with_columns(s.select("shot_value", "x_legacy", "y_legacy"))
    corner = r.filter(pl.col("zone") == "corner_3")
    hypot = (pl.col("x_legacy") ** 2 + pl.col("y_legacy") ** 2).sqrt()
    assert corner.select((pl.col("shot_value") == 3).all(), (pl.col("y_legacy") <= 87.5).all()).row(0) == (True, True)
    assert corner.select(hypot.min()).item() >= line - 10  # a foot of feed rounding inside the line
    if line == 205:
        assert corner.filter(pl.col("x_legacy").abs() < 220).height > corner.height / 2


def test_wnba_placeholders_are_unlocated() -> None:
    """WNBA 2010: the hoop-centre placeholder attempts count in fga and in no zone; 2025 has none."""
    s = _shots("wnba_stats_shots_2010")
    placeholders = s.filter((pl.col("x_legacy") == 0) & (pl.col("y_legacy") == 0)).height
    assert placeholders == 944  # observed on the fixture
    r = _rows("wnba_stats_shots_2010", "10")
    assert r["zone"].null_count() == placeholders
    teams = team_shot_zones(r, level="wnba")
    assert teams["unlocated_fga"].sum() == placeholders
    assert _rows("wnba_stats_shots_2025", "10")["zone"].null_count() == 0
    # the same (0, 0) attempt is a real dunk in the NBA: located
    assert _rows("nba_stats_shots_2025", "00")["zone"].null_count() == 0


def test_assisted_reads_the_description() -> None:
    """A make is assisted exactly when its description credits an assist."""
    s = _shots("nba_stats_shots_2025")
    r = _rows("nba_stats_shots_2025", "00")
    made = s["shot_result"] == "Made"
    credited = s.filter(made & s["description"].str.contains(r"\(\S.* \d+ AST\)"))
    assert r["assisted"].sum() == credited.height
    assert r.filter(~pl.col("made"))["assisted"].null_count() == r.filter(~pl.col("made")).height
    assert r.filter(pl.col("made"))["assisted"].null_count() == 0
    players = player_shot_zones(r, level="nba")
    assert players.select(sum(pl.col(f"{z}_ast") for z in ZONES).sum()).item() == credited.height


def test_ranks_follow_bb_d2() -> None:
    """Gated ranks: 1 = highest, the allowed fg% lowest first, never-half-signal rates unranked."""
    teams = team_shot_zones(_rows("nba_stats_shots_2025", "00"), level="nba")
    for m in zone_columns() + [f"opp_{m}" for m in zone_columns()]:
        assert {f"{m}_n", f"{m}_rank", f"{m}_pct"} <= set(teams.columns)
    ranked = teams.filter(pl.col("rim_freq_rank").is_not_null())
    assert ranked.height >= 5
    assert ranked.sort("rim_freq", descending=True)["rim_freq_rank"][0] == 1.0
    stingy = teams.filter(pl.col("opp_mid_freq_rank").is_not_null())
    n = stingy.height
    assert (stingy["opp_mid_freq_pct"] - 100 * (n + 1 - stingy["opp_mid_freq_rank"]) / (n + 1)).abs().max() < 1e-9
    # a three-point make rate never reaches half signal inside a season: never ranked
    assert teams["corner_3_fg_pct_rank"].null_count() == teams.height
    # the _n is the rate's own denominator
    assert (teams["rim_fg_pct_n"] == teams["rim_fga"]).all()
    assert (teams["rim_freq_n"] == teams["fga"] - teams["unlocated_fga"]).all()


def test_allowed_fg_pct_ranks_lowest_first(monkeypatch: pytest.MonkeyPatch) -> None:
    """The stingiest defense is opp_*_fg_pct rank 1; the team's own fg% ranks highest first.

    The fixture's 120 games are under the fitted rim gates, so the gates are lowered here: the test
    is about the direction, which the gates do not change.
    """
    from sportsdataverse._common import shot_zone_profiles as szp

    monkeypatch.setitem(szp.TEAM_MIN_N, "nba", {"rim_fg_pct": 1, "opp_rim_fg_pct": 1})
    teams = team_shot_zones(_rows("nba_stats_shots_2025", "00"), level="nba")
    assert teams["opp_rim_fg_pct_rank"].null_count() == 0
    assert teams.sort("opp_rim_fg_pct")["opp_rim_fg_pct_rank"][0] == 1.0
    assert teams.sort("rim_fg_pct", descending=True)["rim_fg_pct_rank"][0] == 1.0


def test_empty_input_keeps_the_schema() -> None:
    """No attempts: zero rows with every column."""
    empty = nsz.nba_shot_zone_rows(_shots("nba_stats_shots_2025").head(0), league_id="00")
    assert empty.schema == pl.Schema(SHOT_ROWS_SCHEMA)
    players, teams = player_shot_zones(empty, level="nba"), team_shot_zones(empty, level="nba")
    assert players.height == teams.height == 0
    assert "rim_freq_pct" in players.columns and "opp_rim_freq_pct" in teams.columns


def test_bad_league_id_raises_before_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unknown league_id raises before the release is read."""
    import sportsdataverse.nba.nba_loaders as L

    monkeypatch.setattr(L, "load_nba_stats_shots", lambda *a, **k: pytest.fail("loaded"))
    with pytest.raises(ValueError, match="league_id"):
        nsz.nba_player_shot_zones(2025, league_id="20")


def test_producers_load_the_named_season(monkeypatch: pytest.MonkeyPatch) -> None:
    """The NBA loader takes the START year; the WNBA one the year itself."""
    import sportsdataverse.nba.nba_loaders as L
    import sportsdataverse.wnba.wnba_loaders as W

    calls: list[tuple[str, list[int]]] = []

    def fake(name: str, fixture: str):  # type: ignore[no-untyped-def]
        def load(seasons, return_as_pandas=False):  # type: ignore[no-untyped-def]
            calls.append((name, list(seasons)))
            return _shots(fixture)

        return load

    monkeypatch.setattr(L, "load_nba_stats_shots", fake("nba", "nba_stats_shots_2025"))
    monkeypatch.setattr(W, "load_wnba_stats_shots", fake("wnba", "wnba_stats_shots_2025"))
    players = nsz.nba_player_shot_zones(2025)
    teams = nsz.nba_team_shot_zones(2025, league_id="10")
    assert calls == [("nba", [2024]), ("wnba", [2025])]
    assert players["season"].unique().to_list() == [2025]
    assert teams.height == teams["team_id"].n_unique() and teams["team_id"].null_count() == 0
    from sportsdataverse.wnba.wnba_shot_zone_profiles import wnba_team_shot_zones

    assert wnba_team_shot_zones(2025).equals(teams)
