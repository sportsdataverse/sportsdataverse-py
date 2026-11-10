"""BB-D35: college season shot profile by zone on committed real captures.

Fixtures (``tests/fixtures/bb_shot_zones``, README there; none synthetic): the first 80 contests of
the 2024-25 ``ncaa_{mbb,wbb}_shots`` releases, in release order, and the field-goal rows of the
same contests from ``ncaa_{mbb,wbb}_pbp``, in release order.

The gates: the four zone counts plus ``unlocated_fga`` add up to ``fga`` for players, teams and
opponents; a corner three sits beyond the 21 ft 7 7/8 in corner in the baseline band; and the
chart-to-play-by-play assist link agrees with the play-by-play's own assist count.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.shot_zone_profiles import ZONES, player_shot_zones, team_shot_zones
from sportsdataverse.mbb import mbb_shot_zone_profiles as msz

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_shot_zones"
LEAGUES = [("mbb", "mens"), ("wbb", "womens")]


def _inputs(lg: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The committed shot-chart and play-by-play captures of a league."""
    return pl.read_parquet(FIX / f"ncaa_{lg}_shots_2025.parquet"), pl.read_parquet(
        FIX / f"ncaa_{lg}_pbp_fg_2025.parquet"
    )


def _zone_sum(prefix: str = "") -> pl.Expr:
    """The four zone attempt counts plus the unlocated ones."""
    return sum((pl.col(f"{prefix}{z}_fga") for z in ZONES), pl.col(f"{prefix}unlocated_fga"))


@pytest.mark.parametrize(("lg", "league"), LEAGUES)
def test_zone_attempts_add_up_to_fga(lg: str, league: str) -> None:
    """Every attempt lands in one zone or is unlocated; non-D1 attempts count against the D1 side."""
    shots, pbp = _inputs(lg)
    rows = msz.ncaa_shot_zone_rows(shots, pbp)
    assert rows.schema == pl.Schema(msz.SCHEMA)
    assert rows.height == shots.height
    players, teams = player_shot_zones(rows, level=lg), team_shot_zones(rows, level=lg)
    assert players.select((_zone_sum() == pl.col("fga")).all()).item()
    assert teams.select((_zone_sum() == pl.col("fga")).all() & (_zone_sum("opp_") == pl.col("opp_fga")).all()).item()
    # a D1 team's attempts count for it; every attempt against a D1 side counts against it,
    # a non-D1 shooter's included
    assert teams["fga"].sum() == rows.filter(pl.col("team_id").is_not_null()).height
    assert teams["opp_fga"].sum() == rows.filter(pl.col("opp_team_id").is_not_null()).height
    assert rows.filter(pl.col("team_id").is_null() & pl.col("opp_team_id").is_not_null()).height > 0
    assert players["player_id"].dtype == pl.Utf8 and teams["team_id"].dtype == pl.Int64


@pytest.mark.parametrize(("lg", "league"), LEAGUES)
def test_corner_three_geometry(lg: str, league: str) -> None:
    """A corner three is a three from the baseline band at the 21 ft 7 7/8 in corner (the chart puts
    lateral positions on a half-foot grid, so the corner reads 21.0 or 21.5 ft)."""
    shots, pbp = _inputs(lg)
    r = msz.ncaa_shot_zone_rows(shots, pbp).with_columns(shots.select("point_value", "shot_x", "shot_y", "dist_ft"))
    corner = r.filter(pl.col("zone") == "corner_3")
    assert corner.height > 0
    assert corner.select(
        (pl.col("point_value") == 3).all(),
        (pl.col("shot_x").abs() >= 21).all(),
        (pl.col("shot_y").abs() <= 9).all(),
        (pl.col("dist_ft") >= 21).all(),  # the 21.66 ft corner on the chart's half-foot lateral grid
    ).row(0) == (True, True, True, True)
    threes = r.filter(pl.col("point_value") == 3)
    assert threes["zone"].is_in(["corner_3", "above_break_3"]).all()
    assert r.filter(pl.col("point_value") == 2)["zone"].is_in(["rim", "mid"]).all()


@pytest.mark.parametrize(("lg", "league"), LEAGUES)
def test_assist_link_agrees_with_the_pbp(lg: str, league: str) -> None:
    """Linked makes: 99%+ of the makes; the assisted count is the pbp's, give or take the unlinked."""
    shots, pbp = _inputs(lg)
    rows = msz.ncaa_shot_zone_rows(shots, pbp)
    made = rows.filter(pl.col("made"))
    unlinked = made["assisted"].null_count()
    assert unlinked / made.height < 0.01  # observed: mbb 0.4%, wbb 0.5% of the fixture's makes
    pbp_assisted = pbp.filter((pl.col("event_result") == "made") & pl.col("assist_player").is_not_null()).height
    assert abs(made["assisted"].sum() - pbp_assisted) <= unlinked
    assert rows.filter(~pl.col("made"))["assisted"].null_count() == rows.filter(~pl.col("made")).height
    # with no play-by-play every assist is unknown, so no assisted share is computed
    bare = player_shot_zones(msz.ncaa_shot_zone_rows(shots, pbp.head(0)), level=lg)
    assert bare.select(pl.col("rim_ast_pct").is_null().all(), (pl.col("rim_ast_pct_n") == 0).all()).row(0) == (
        True,
        True,
    )


def test_a_shifted_pbp_is_not_linked() -> None:
    """The link checks made / value / shooter: one pbp row dropped per contest shifts the k-th row and
    unlinks most of the makes, instead of crediting the wrong assists."""
    shots, pbp = _inputs("mbb")
    shifted = pbp.filter(pl.int_range(pl.len()).over("contest_id") > 0)
    made = msz.ncaa_shot_zone_rows(shots, shifted).filter(pl.col("made"))
    assert made["assisted"].null_count() / made.height > 0.3


def test_bad_league_raises_before_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unknown league raises before the releases are read."""
    import sportsdataverse.mbb.mbb_loaders as L

    monkeypatch.setattr(L, "load_ncaa_mbb_shots", lambda *a, **k: pytest.fail("loaded"))
    with pytest.raises(ValueError, match="league"):
        msz.mbb_team_shot_zones(2025, league="coed")


def test_producers_read_the_league_releases(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each producer reads its own league's shot and play-by-play releases."""
    import sportsdataverse.mbb.mbb_loaders as L
    import sportsdataverse.wbb.wbb_loaders as W

    calls: list[str] = []

    def fake(name: str, frame: pl.DataFrame):  # type: ignore[no-untyped-def]
        def load(seasons, return_as_pandas=False):  # type: ignore[no-untyped-def]
            calls.append(f"{name}:{list(seasons)}")
            return frame

        return load

    for mod, lg in ((L, "mbb"), (W, "wbb")):
        shots, pbp = _inputs(lg)
        monkeypatch.setattr(mod, f"load_ncaa_{lg}_shots", fake(f"{lg}_shots", shots))
        monkeypatch.setattr(mod, f"load_ncaa_{lg}_pbp", fake(f"{lg}_pbp", pbp))
    from sportsdataverse.wbb.wbb_shot_zone_profiles import wbb_player_shot_zones, wbb_team_shot_zones

    men = msz.mbb_player_shot_zones(2025)
    women = wbb_team_shot_zones(2025)
    wbb_player_shot_zones(2025, return_as_pandas=True)
    assert calls[:4] == ["mbb_shots:[2025]", "mbb_pbp:[2025]", "wbb_shots:[2025]", "wbb_pbp:[2025]"]
    assert men.height > 0 and women.height == women["team_id"].n_unique()
