"""BB-D3: NBA / WNBA season team rates from possessions, on committed real captures.

Fixtures (``tests/fixtures/bb_team_season_rates``, README there; none synthetic):

* ``nba_possessions_2026`` / ``nba_schedule_2026`` -- the 2025-26 ``nba_stats_possessions`` and
  ``nba_stats_schedules`` releases (regular season plus preseason), projected to the columns read.
* ``nba_team_stats_advanced_2026`` -- stats.nba.com ``team_stats_advanced`` (Regular Season), the
  net-rating oracle (BB-D3's done-when: every team within 1.0).
* ``nba_possessions_2024`` / ``nba_schedule_2024`` + ``nba_prediction/clutch_team_2024`` --
  stats.nba.com's clutch dashboard, the oracle for the clutch window.
* ``wnba_*_2025`` -- the WNBA twins and ``team_stats_advanced``.

The ``bb-team-season-rates`` rows of ``tests/mutants/registry.yaml`` go red here: the points
formula, the possession count, the for/against swap, the home/away split, and the clutch
window's clock, margin and period.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.league_context import LEAGUE_AVERAGES_SCHEMA
from sportsdataverse._common.rate_gates import TEAM_MIN_N
from sportsdataverse._common.team_season_rates import (
    SPLITS,
    TEAM_SEASON_RATES_SCHEMA,
    team_season_rates,
    team_season_rates_league_averages,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
RATES = FIXTURES / "bb_team_season_rates"
# The package re-exports the function under the module's name, so import the module itself.
mod = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")


def _fixture_loader(name: str, expect: list[int]):  # type: ignore[no-untyped-def]
    frame = pl.read_parquet(RATES / name)

    def load(seasons: list[int]) -> pl.DataFrame:
        assert list(seasons) == expect, f"{name}: loader called with {seasons}, expected {expect}"
        return frame

    return load


def _nba(monkeypatch: pytest.MonkeyPatch, season: int, **kwargs: object) -> pl.DataFrame:
    # The stats loaders take the START year; the table is named by the END year.
    monkeypatch.setattr(
        mod, "load_nba_stats_possessions", _fixture_loader(f"nba_possessions_{season}.parquet", [season - 1])
    )
    monkeypatch.setattr(
        mod, "load_nba_stats_schedules", _fixture_loader(f"nba_schedule_{season}.parquet", [season - 1])
    )
    return mod.nba_team_season_rates(season, **kwargs)  # type: ignore[arg-type]


@pytest.fixture(scope="module")
def nba_2026() -> pl.DataFrame:
    with pytest.MonkeyPatch.context() as mp:
        return _nba(mp, 2026)


def _oracle(name: str) -> pl.DataFrame:
    return pl.read_parquet(RATES / name)


def _join_oracle(rates: pl.DataFrame, oracle: pl.DataFrame) -> pl.DataFrame:
    left = rates.filter(split="all")
    assert left.schema["team_id"] == oracle.schema["team_id"], "team_id dtype differs from the oracle"
    out = left.join(oracle, on="team_id", how="inner", validate="1:1", suffix="_oracle")
    assert out.height == left.height == oracle.height
    return out


def test_schema_and_shape(nba_2026: pl.DataFrame) -> None:
    assert dict(nba_2026.schema) == TEAM_SEASON_RATES_SCHEMA
    assert nba_2026.height == 30 * len(SPLITS)
    assert nba_2026["season"].unique().to_list() == [2026]
    assert nba_2026.group_by("split").len()["len"].to_list() == [30] * len(SPLITS)


def test_nba_2026_net_rating_parity(nba_2026: pl.DataFrame) -> None:
    """BB-D3 done-when: every team's net rating within 1.0 of stats.nba.com team_stats_advanced."""
    j = _join_oracle(nba_2026, _oracle("nba_team_stats_advanced_2026.parquet"))
    delta = (j["net_rtg"] - j["net_rating"]).abs()
    assert delta.max() <= 1.0, j.select("team_name", "net_rtg", "net_rating").filter(delta > 1.0)
    # The ratings move with stats.nba.com on both ends, not only in the difference.
    assert (j["off_rtg"] - j["off_rating"]).abs().max() <= 1.5
    assert (j["def_rtg"] - j["def_rating"]).abs().max() <= 1.5


def test_nba_2026_possessions_pace_and_four_factors(nba_2026: pl.DataFrame) -> None:
    j = _join_oracle(nba_2026, _oracle("nba_team_stats_advanced_2026.parquet"))
    assert (j["games"] == j["gp"]).all()
    # Possession rows, one per possession: within 2% of stats.nba.com's possession count.
    assert ((j["poss"] - j["poss_oracle"]).abs() / j["poss_oracle"]).max() <= 0.02
    assert (j["pace"] - j["pace_oracle"]).abs().max() <= 1.0
    assert (j["efg_pct"] - j["efg_pct_oracle"]).abs().max() <= 0.002
    assert (j["tov_pct"] - j["tm_tov_pct"]).abs().max() <= 0.005
    assert (j["orb_pct"] - j["oreb_pct"]).abs().max() <= 0.02


def test_for_and_against_balance(nba_2026: pl.DataFrame) -> None:
    """Every possession is one team's offense and the other's defense, league-wide."""
    for split in ("all", "clutch"):
        s = nba_2026.filter(split=split)
        assert s["poss"].sum() == s["opp_poss"].sum()
        assert s["pts"].sum() == s["opp_pts"].sum()
    # pts / opp_pts are what the team / its opponents scored: the schedule's box score, short only
    # the few free throws the possession engine leaves unattributed (never over the box).
    sched = pl.read_parquet(RATES / "nba_schedule_2026.parquet").filter(season_type="regular-season")
    box = (
        pl.concat(
            [
                sched.select(
                    pl.col("home_team_id").alias("team_id"),
                    pl.col("home_pts").alias("for"),
                    pl.col("away_pts").alias("against"),
                ),
                sched.select(
                    pl.col("away_team_id").alias("team_id"),
                    pl.col("away_pts").alias("for"),
                    pl.col("home_pts").alias("against"),
                ),
            ]
        )
        .group_by("team_id")
        .agg(pl.col("for").sum(), pl.col("against").sum())
    )
    j = nba_2026.filter(split="all").join(box, on="team_id", validate="1:1")
    assert j.height == 30
    for ours, theirs in (("pts", "for"), ("opp_pts", "against")):
        short = (j[theirs] - j[ours]) / j[theirs]
        assert short.min() >= 0 and short.max() <= 0.01, (ours, short.min(), short.max())


def test_home_away_split_against_the_box_score(nba_2026: pl.DataFrame) -> None:
    from sportsdataverse.nba.nba_neutral_site import nba_neutral_site_games

    # 2025-26's neutral games (Mexico City, Berlin, London, the Cup semifinals) are in neither split
    neutral = nba_neutral_site_games([2026])["game_id"].to_list()
    sched = pl.read_parquet(RATES / "nba_schedule_2026.parquet").filter(season_type="regular-season")
    is_neutral = pl.col("game_id").cast(pl.Utf8).is_in(neutral)
    home_box = (
        sched.filter(~is_neutral).group_by(pl.col("home_team_id").alias("team_id")).agg(pl.len().alias("home_games"))
    )
    n_neutral = (
        pl.concat([sched.filter(is_neutral).select(pl.col(f"{s}_team_id").alias("team_id")) for s in ("home", "away")])
        .group_by("team_id")
        .agg(pl.len().alias("neutral_games"))
    )
    assert n_neutral["neutral_games"].sum() == 2 * 5  # 3 games abroad and the 2 semifinals, both sides
    wide = (
        nba_2026.filter(pl.col("split").is_in(["all", "home", "away"]))
        .pivot(on="split", index="team_id", values=["games", "poss", "pts"])
        .join(home_box, on="team_id", validate="1:1")
        .join(n_neutral, on="team_id", how="left", validate="1:1")
        .with_columns(pl.col("neutral_games").fill_null(0))
    )
    assert (wide["games_home"] == wide["home_games"]).all()
    assert (wide["games_home"] + wide["games_away"] + wide["neutral_games"] == wide["games_all"]).all()
    plain = wide.filter(pl.col("neutral_games") == 0)
    assert (plain["poss_home"] + plain["poss_away"] == plain["poss_all"]).all()
    assert (plain["pts_home"] + plain["pts_away"] == plain["pts_all"]).all()
    played = wide.filter(pl.col("neutral_games") > 0)  # DET DAL ORL MEM NYK OKC SAS: neutral poss in `all` only
    assert played.height == 7 and (played["poss_home"] + played["poss_away"] < played["poss_all"]).all()
    # Home-court advantage is real in 2025-26: the league's home split outscores its away split.
    home = nba_2026.filter(split="home")
    away = nba_2026.filter(split="away")
    assert home["net_rtg"].mean() > 1.0 > -1.0 > away["net_rtg"].mean()


def test_season_type_filter(monkeypatch: pytest.MonkeyPatch, nba_2026: pl.DataFrame) -> None:
    assert nba_2026.filter(split="all")["games"].max() == 82
    pre = _nba(monkeypatch, 2026, season_type="preseason")
    assert 0 < pre.filter(split="all")["games"].max() <= 6


def test_clutch_window_against_stats_nba_2024(monkeypatch: pytest.MonkeyPatch) -> None:
    """The clutch split matches stats.nba.com's clutch dashboard (last 5:00, within 5)."""
    rates = _nba(monkeypatch, 2024).filter(split="clutch")
    oracle = pl.read_parquet(FIXTURES / "nba_prediction" / "clutch_team_2024.parquet").with_columns(
        pl.col("team_id").cast(pl.Int64)
    )
    assert rates.schema["team_id"] == oracle.schema["team_id"]
    j = rates.join(oracle, on="team_id", validate="1:1")
    assert j.height == 30
    assert j.select(pl.corr("poss", "clutch_poss")).item() >= 0.99
    # observed: stats.nba.com counts 12 more a team (23 before the possession live at 5:00 counted)
    assert 10.0 <= (j["clutch_poss"] - j["poss"]).mean() <= 14.0
    assert j.select(pl.corr("net_rtg", "clutch_net_rating")).item() >= 0.99  # observed 0.992


def test_close_games_are_the_clutch_games(nba_2026: pl.DataFrame) -> None:
    wide = nba_2026.pivot(on="split", index="team_id", values="games")
    assert (wide["close"] == wide["clutch"]).all()
    assert (wide["close"] < wide["all"]).all()
    # ~7 clutch possessions per clutch game: the last five minutes of the 4th quarter and overtime.
    clutch = nba_2026.filter(split="clutch")
    per_game = (clutch["poss"] / clutch["games"]).mean()
    assert 5.0 <= per_game <= 10.0, per_game


def test_ranks_percentiles_and_sample_sizes(nba_2026: pl.DataFrame) -> None:
    all_ = nba_2026.filter(split="all")
    assert all_.filter(pl.col("net_rtg_rank") == 1)["net_rtg"].item() == all_["net_rtg"].max()
    assert all_.filter(pl.col("def_rtg_rank") == 1)["def_rtg"].item() == all_["def_rtg"].min()
    assert all_.filter(pl.col("opp_efg_pct_rank") == 1)["opp_efg_pct"].item() == all_["opp_efg_pct"].min()
    assert all_.filter(pl.col("tov_pct_rank") == 1)["tov_pct"].item() == all_["tov_pct"].min()
    assert all_.filter(pl.col("opp_tov_pct_rank") == 1)["opp_tov_pct"].item() == all_["opp_tov_pct"].max()
    assert all_["net_rtg_pct"].max() == pytest.approx(100 * 30 / 31)
    assert (all_["off_rtg_n"] == all_["poss"]).all()
    assert (all_["def_rtg_n"] == all_["opp_poss"]).all()
    assert (all_["net_rtg_n"] == all_["poss"] + all_["opp_poss"]).all()
    # pace's sample is the clock its possessions used, in 48-minute games: the games plus overtime
    assert ((all_["pace_n"] - all_["games"]).abs() <= 1).all()
    clutch = nba_2026.filter(split="clutch")
    assert (clutch["pace_n"] < clutch["games"] / 4).all()  # five-minute windows, not games
    # eFG% and FT rate share the FGA denominator; ORB% counts chances, not possessions.
    assert (all_["efg_pct_n"] == all_["ft_rate_n"]).all()
    assert (all_["efg_pct_n"] > all_["poss"] * 0.8).all()
    assert (all_["orb_pct_n"] < all_["poss"]).all()


def test_sample_gates_rank_only_half_signal_rates(nba_2026: pl.DataFrame) -> None:
    """``level`` gates: a rate is ranked only where its ``<m>_n`` reaches the fitted r = 0.5 sample."""
    # a scoring basis is gated where its rating is (BB-G8)
    gates = {**TEAM_MIN_N["nba"], **{m: TEAM_MIN_N["nba"][r] for m, r in _BASIS_RATING.items()}}
    rated = [c.removesuffix("_rank") for c in TEAM_SEASON_RATES_SCHEMA if c.endswith("_rank")]
    assert set(rated) <= set(gates)
    for m in rated:
        under = nba_2026.filter(pl.col(f"{m}_n") < gates[m])
        assert under[f"{m}_rank"].null_count() == under.height, m
        assert under[f"{m}_pct"].null_count() == under.height, m
    # A full season clears every gate; the clutch split (a few hundred possessions) clears none.
    assert nba_2026.filter(split="all").select(pl.col("^.*_rank$").null_count()).sum_horizontal().item() == 0
    clutch = nba_2026.filter(split="clutch")
    assert clutch.select(pl.col("^.*_rank$").is_not_null().sum()).sum_horizontal().item() == 0
    # ungated, the clutch split ranks its ratings: the adapter's gate is what nulls them
    ungated = team_season_rates(
        mod.nba_possession_rate_rows(
            pl.read_parquet(RATES / "nba_possessions_2026.parquet"),
            pl.read_parquet(RATES / "nba_schedule_2026.parquet"),
        ),
        minutes_per_game=48,
    )
    ungated_clutch = ungated.filter(split="clutch")
    assert ungated_clutch["net_rtg_rank"].null_count() < ungated_clutch.height


#: BB-G8's scoring bases -> the rating whose sample and gate rank them.
_BASIS_RATING = {
    "pts_pg": "off_rtg",
    "opp_pts_pg": "def_rtg",
    "plus_minus_pg": "net_rtg",
    "pts_per75": "off_rtg",
    "opp_pts_per75": "def_rtg",
    "plus_minus_per75": "net_rtg",
}


def test_scoring_bases_per_game_and_per_75(nba_2026: pl.DataFrame) -> None:
    """BB-G8: per game against the final scores, per 75 = 0.75 x per 100 with the rating's rank."""
    sched = pl.read_parquet(RATES / "nba_schedule_2026.parquet").filter(season_type="regular-season")
    sides = [("home", "away"), ("away", "home")]
    box = (
        pl.concat(
            [
                sched.select(
                    pl.col(f"{us}_team_id").alias("team_id"),
                    pl.col(f"{us}_pts").alias("p"),
                    pl.col(f"{them}_pts").alias("o"),
                )
                for us, them in sides
            ]
        )
        .group_by("team_id")
        .agg(pl.col("p").mean(), pl.col("o").mean())
    )
    all_ = nba_2026.filter(split="all")
    assert all_.schema["team_id"] == box.schema["team_id"]
    j = all_.join(box, on="team_id", how="inner", validate="1:1")
    assert j.height == 30
    assert (j["plus_minus"] == j["pts"] - j["opp_pts"]).all()
    # possession points against the final score: within a third of a point a game (scoring corrections)
    assert (j["pts_pg"] - j["p"]).abs().max() < 0.35
    assert (j["opp_pts_pg"] - j["o"]).abs().max() < 0.35
    assert (j["plus_minus_pg"] - (j["p"] - j["o"])).abs().max() < 0.35
    for split in SPLITS:
        t = nba_2026.filter(split=split)
        assert (t["pts_pg"] - t["pts"] / t["games"]).abs().max() < 1e-9, split
        for m, r in (("pts_per75", "off_rtg"), ("opp_pts_per75", "def_rtg"), ("plus_minus_per75", "net_rtg")):
            assert (t[m] - 0.75 * t[r]).abs().max() < 1e-9, (split, m)
            for s in ("rank", "pct", "n"):
                assert t[f"{m}_{s}"].equals(t[f"{r}_{s}"]), (split, m, s)
        for m, r in _BASIS_RATING.items():
            assert t[f"{m}_n"].equals(t[f"{r}_n"]), (split, m)
    # the per-game ranks: direction, contiguity (1..30, ties averaged) and the cohort percentile
    for m, best in (("pts_pg", "max"), ("opp_pts_pg", "min"), ("plus_minus_pg", "max")):
        assert all_.filter(pl.col(f"{m}_rank") == 1)[m].item() == getattr(all_[m], best)(), m
        assert all_[f"{m}_rank"].sum() == 30 * 31 / 2, m
        assert all_[f"{m}_pct"].max() == pytest.approx(100 * 30 / 31), m
    # per game is not per possession: pace reorders them
    assert not all_["pts_pg_rank"].equals(all_["off_rtg_rank"])


def test_league_averages_n_is_the_cohort(nba_2026: pl.DataFrame) -> None:
    avg = team_season_rates_league_averages(nba_2026, level="nba")
    assert dict(avg.schema) == LEAGUE_AVERAGES_SCHEMA
    # the clutch split's rates clear no sample gate, so it has no league-average row at all
    assert sorted(avg["category"].unique()) == sorted(f"team_season_rates_{s}" for s in SPLITS if s != "clutch")
    assert (avg["n"] == 30).all()
    net = avg.filter(category="team_season_rates_all", metric="net_rtg")
    assert net["mean"].item() == pytest.approx(nba_2026.filter(split="all")["net_rtg"].mean())


def test_wnba_2025_parity(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")
    from sportsdataverse.wnba import wnba_loaders
    from sportsdataverse.wnba.wnba_team_season_rates import wnba_team_season_rates

    seen: list[object] = []
    real = adapter.team_season_rates

    def spy(*args: object, **kwargs: object) -> pl.DataFrame:
        seen.append(kwargs["level"])
        return real(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(adapter, "team_season_rates", spy)

    monkeypatch.setattr(
        wnba_loaders, "load_wnba_stats_possessions", _fixture_loader("wnba_possessions_2025.parquet", [2025])
    )
    monkeypatch.setattr(
        wnba_loaders, "load_wnba_stats_schedules", _fixture_loader("wnba_schedule_2025.parquet", [2025])
    )
    rates = wnba_team_season_rates(2025)
    assert seen == ["wnba"]  # the WNBA level: a stricter (NBA) gate would pass the null checks below
    assert rates.height == 13 * len(SPLITS)
    for m, gate in TEAM_MIN_N["wnba"].items():  # the WNBA adapter gates on the WNBA's own samples
        if f"{m}_rank" in rates.columns:
            under = rates.filter(pl.col(f"{m}_n") < gate)
            assert under[f"{m}_rank"].null_count() == under.height, m
    assert rates.filter(split="clutch")["net_rtg_rank"].null_count() == 13
    j = _join_oracle(rates, _oracle("wnba_team_stats_advanced_2025.parquet"))
    assert (j["games"] == j["gp"]).all()
    assert (j["net_rtg"] - j["net_rating"]).abs().max() <= 1.0
    assert (j["pace"] - j["pace_per40"]).abs().max() <= 1.5
    playoffs = wnba_team_season_rates(2025, season_type="playoffs")
    assert playoffs.filter(split="all").height == 8


def test_empty_input_keeps_the_schema() -> None:
    from sportsdataverse._common.team_season_rates import RATE_ROWS_SCHEMA

    out = team_season_rates(pl.DataFrame(schema=RATE_ROWS_SCHEMA), minutes_per_game=48)
    assert out.height == 0 and dict(out.schema) == TEAM_SEASON_RATES_SCHEMA
    rows = mod.nba_possession_rate_rows(pl.DataFrame(), pl.DataFrame())
    assert dict(rows.schema) == RATE_ROWS_SCHEMA
    # a counts frame missing columns raises even when there are no possessions to rate
    with pytest.raises(KeyError, match="counts missing"):
        team_season_rates(pl.DataFrame(schema=RATE_ROWS_SCHEMA), minutes_per_game=40, counts=pl.DataFrame())


def test_bad_league_id_raises() -> None:
    with pytest.raises(ValueError, match="league_id"):
        mod.nba_team_season_rates(2026, league_id="20")
    with pytest.raises(ValueError, match="season_type"):
        mod.nba_team_season_rates(2026, season_type="regular_season")
