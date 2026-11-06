"""BB-D4: college lineup seasons, on committed real captures.

Fixtures (``tests/fixtures/bb_lineup_seasons``, README there; none synthetic): the BB-D6 contests of
2024-25 (12 men's D1 games plus one against a non-D1 visitor, 12 women's) from the
``ncaa_{mbb,wbb}_pbp`` and ``ncaa_{mbb,wbb}_possessions`` releases, plus the same contests'
``ncaa_{mbb,wbb}_team_box``, the count oracle: summed per team and game, the lineup counts equal
it exactly (every count, every D1 team-game; the same holds for all 11,957 D1 men's team-games of
2024-25). The ``bb-lineup-seasons`` rows of ``tests/mutants/registry.yaml`` go red here.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.lineup_seasons import lineup_seasons
from sportsdataverse.mbb import mbb_loaders
from sportsdataverse.mbb.mbb_game_impact import ncaa_impact_rows
from sportsdataverse.mbb.mbb_lineup_seasons import mbb_lineup_seasons, ncaa_lineup_count_rows
from sportsdataverse.wbb import wbb_loaders
from sportsdataverse.wbb.wbb_lineup_seasons import wbb_lineup_seasons

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_lineup_seasons"
LEAGUES = {"mbb": "mens", "wbb": "womens"}
#: count -> its ncaa_*_team_box column.
BOX = {
    "fga": "fga", "fgm": "fgm", "fg3a": "tpa", "fg3m": "tpm", "fta": "fta",
    "oreb": "orb", "dreb": "drb", "tov": "to", "rima": "rima", "rimm": "rimm",
}  # fmt: skip
_SLOTS = [f"{s}_{i}" for s in ("home", "away") for i in range(1, 6)]
#: The rank direction of every rate: True when the lowest value ranks 1 (written out, not imported).
LOW_FIRST = {
    "off_rtg": False, "def_rtg": True, "net_rtg": False, "efg_pct": False, "tov_pct": True,
    "orb_pct": False, "ft_rate": False, "opp_efg_pct": True, "opp_tov_pct": False,
    "opp_orb_pct": True, "opp_ft_rate": True, "rim_rate": False, "mid_rate": False,
    "fg3_rate": False, "rim_pct": False, "mid_pct": False, "fg3_pct": False,
    "opp_rim_rate": False, "opp_mid_rate": False, "opp_fg3_rate": False, "opp_rim_pct": True,
    "opp_mid_pct": True, "opp_fg3_pct": True,
}  # fmt: skip


def _read(lg: str, name: str) -> pl.DataFrame:
    return pl.read_parquet(FIX / f"ncaa_{lg}_{name}_2025.parquet")


@cache
def _built(lg: str) -> pl.DataFrame:
    pbp = _read(lg, "pbp")
    rows = ncaa_impact_rows(pbp, _read(lg, "possessions"), league=LEAGUES[lg])
    return lineup_seasons(rows, ncaa_lineup_count_rows(pbp), player_dtype=pl.Utf8(), min_poss=20, zones=True)


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_counts_equal_the_team_box(lg: str) -> None:
    counts = ncaa_lineup_count_rows(_read(lg, "pbp"))
    per_side = [
        counts.group_by("game_id", pl.col(f"{s}_team_id").alias("team_id")).agg(
            pl.col(f"{s}_{c}").sum().alias(c) for c in BOX
        )
        for s in ("home", "away")
    ]
    ours = pl.concat(per_side).filter(pl.col("team_id").is_not_null())
    box = _read(lg, "team_box").select(
        pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
        pl.col("team_espn_team_id").cast(pl.Int64).alias("team_id"),
        *[pl.col(b).cast(pl.Int64).alias(f"box_{c}") for c, b in BOX.items()],
    )
    assert ours.schema["team_id"] == box.schema["team_id"] and ours.schema["game_id"] == box.schema["game_id"]
    both = ours.join(box, on=["game_id", "team_id"], how="inner", validate="1:1")
    assert both.height == ours.height == {"mbb": 25, "wbb": 24}[lg]  # every D1 team-game (25 men's: one non-D1 side)
    for c in BOX:
        assert (both[c] == both[f"box_{c}"]).all(), f"{lg} {c}"


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_free_throws_count_on_the_foul_lineup(lg: str) -> None:
    pbp = _read(lg, "pbp")
    slots = [f"{c}_player_id" for c in _SLOTS]
    want, moved, foul = [], 0, {}
    for row in pbp.select("contest_id", "event_type", "foul_class", *slots).iter_rows(named=True):
        lineup = tuple(row[c] for c in slots)
        if row["foul_class"] is not None:
            foul[row["contest_id"]] = lineup
        elif row["event_type"] == "Free Throw":
            at_foul = foul.get(row["contest_id"], lineup)
            moved += at_foul != lineup
            want.append(at_foul)
    counts = ncaa_lineup_count_rows(pbp).filter((pl.col("home_fta") + pl.col("away_fta")) > 0)
    got = list(counts.select(_SLOTS).iter_rows())
    assert got == want
    assert moved == {"mbb": 49, "wbb": 32}[lg]  # observed: free throws whose lineup changed after the foul


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_units_add_up_and_zones_split_the_attempts(lg: str) -> None:
    out = _built(lg)
    assert out["team_id"].null_count() == 0 and out["n_players"].unique().sort().to_list() == [2, 3, 5]
    cols = ["minutes", "poss", "opp_poss", "pts", "opp_pts", "rim_pct_n", "mid_pct_n", "fg3_pct_n", "opp_rim_pct_n"]
    per = out.group_by("team_id", "n_players").agg(pl.col(cols).sum())
    fives = per.filter(pl.col("n_players") == 5).drop("n_players")
    for k in (2, 3):
        both = fives.join(per.filter(pl.col("n_players") == k).drop("n_players"), on="team_id", suffix="_k")
        assert both.height == fives.height > 0
        for c in cols:
            assert ((both[f"{c}_k"] / both[c] - 10).abs() < 1e-5).all(), (
                f"{lg} {k}-man {c}"
            )  # minutes are rounded to 1e-6
    shot = out.filter(pl.col("efg_pct_n") > 0)
    split = shot["rim_rate"] + shot["mid_rate"] + shot["fg3_rate"]
    assert ((split - 1).abs() < 1e-12).all()
    assert (shot["rim_pct_n"] + shot["mid_pct_n"] + shot["fg3_pct_n"] == shot["efg_pct_n"]).all()


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_ranks_follow_the_gate_and_direction(lg: str) -> None:
    out = _built(lg)
    for k in (2, 3):
        units = out.filter(pl.col("n_players") == k)
        ranked = units.filter(pl.col("net_rtg_rank").is_not_null())
        assert ranked.height >= 10 and ranked.filter(pl.col("poss") + pl.col("opp_poss") < 20).height == 0
        for m, low in LOW_FIRST.items():
            rated = units.filter(pl.col(f"{m}_rank").is_not_null())
            best = rated.filter(pl.col(f"{m}_rank") == pl.col(f"{m}_rank").min())[m]
            assert (best == (rated[m].min() if low else rated[m].max())).all(), f"{lg} {k}-man {m}"


@pytest.mark.parametrize("lg", list(LEAGUES))
def test_five_man_rows_hold_the_team_box(lg: str) -> None:
    """A team whose game always listed five players: its five-man rows sum to its team box."""
    pbp = _read(lg, "pbp")
    full = {
        s: pl.concat_list([f"{s}_{i}_player_id" for i in range(1, 6)]).list.drop_nulls().list.n_unique() == 5
        for s in ("home", "away")
    }
    sides = [
        pbp.group_by(pl.col("contest_id").cast(pl.Utf8).alias("game_id")).agg(
            pl.col(f"{s}_espn_team_id").first().cast(pl.Int64).alias("team_id"), full[s].all().alias("whole")
        )
        for s in ("home", "away")
    ]
    games = pl.concat(sides).filter(pl.col("team_id").is_not_null())
    box = _read(lg, "team_box").select(
        pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
        pl.col("team_espn_team_id").cast(pl.Int64).alias("team_id"),
        pl.col("fga").cast(pl.Int64),
        pl.col("rima").cast(pl.Int64),
    )
    # a team counts when every one of its fixture games kept five players on its side
    teams = (
        games.join(box, on=["game_id", "team_id"])
        .group_by("team_id")
        .agg(pl.col("whole").all(), pl.col("fga", "rima").sum())
    )
    ours = _built(lg).filter(pl.col("n_players") == 5).group_by("team_id").agg(pl.col("efg_pct_n", "rim_pct_n").sum())
    both = teams.filter("whole").join(ours, on="team_id", validate="1:1")
    assert both.height == {"mbb": 20, "wbb": 21}[lg]  # observed: D1 teams whose lineup never dropped a player
    assert (both["efg_pct_n"] == both["fga"]).all() and (both["rim_pct_n"] == both["rima"]).all()
    # and a team that lost a player somewhere holds at most its box
    short = teams.filter(~pl.col("whole")).join(ours, on="team_id", validate="1:1")
    assert short.height > 0 and (short["efg_pct_n"] <= short["fga"]).all()


def test_public_builders(monkeypatch: pytest.MonkeyPatch) -> None:
    for module, lg in ((mbb_loaders, "mbb"), (wbb_loaders, "wbb")):
        monkeypatch.setattr(module, f"load_ncaa_{lg}_pbp", lambda seasons, lg=lg: _read(lg, "pbp"))
        monkeypatch.setattr(module, f"load_ncaa_{lg}_possessions", lambda seasons, lg=lg: _read(lg, "possessions"))
    assert mbb_lineup_seasons(2025, min_poss=20).equals(_built("mbb"))
    assert wbb_lineup_seasons(2025, min_poss=20).equals(_built("wbb"))
    default = mbb_lineup_seasons(2025)  # the 200-possession gate leaves 13 games' units below the cohort floor
    assert (
        default.filter(pl.col("n_players") == 5)["net_rtg_rank"].null_count()
        == default.filter(pl.col("n_players") == 5).height
    )
    with pytest.raises(ValueError, match="league"):
        mbb_lineup_seasons(2025, league="other")
    assert ncaa_lineup_count_rows(pl.DataFrame()).height == 0
