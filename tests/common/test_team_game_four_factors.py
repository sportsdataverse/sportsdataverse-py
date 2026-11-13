"""GOP Q20 / Q21: the per-team-game four-factor accounting and the league FT%, on real captures.

Fixtures (``tests/fixtures/bb_team_season_rates``, README there; none synthetic): the 2025-26 NBA
and 2025 WNBA possessions and schedules, and every 2024-25 game of a Big Ten team from the
``ncaa_{mbb,wbb}_possessions`` and ``ncaa_{mbb,wbb}_team_box`` releases. The gate: on every real
game the four factors and the residual add up to the points against a league-average offense, to
float precision, and the residual is the stated possession-gap formula, exactly.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.team_season_rates import (
    FT_POSSESSION_WEIGHT,
    RATE_ROWS_SCHEMA,
    TEAM_GAME_FOUR_FACTORS_SCHEMA,
    season_counts,
    team_game_four_factors,
)

RATES = Path(__file__).resolve().parents[1] / "fixtures" / "bb_team_season_rates"
nba = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")
mbb = importlib.import_module("sportsdataverse.mbb.mbb_team_season_rates")
FACTORS = ("shooting_pts", "turnover_pts", "rebounding_pts", "free_throw_pts")
LEAGUES = ("nba", "wnba", "mbb", "wbb")


def _load(name: str):  # type: ignore[no-untyped-def]
    frame = pl.read_parquet(RATES / name)
    return lambda seasons: frame


def _inputs(lg: str) -> tuple[pl.DataFrame, pl.DataFrame | None]:
    """(possession rows, box counts) through the league adapters, as the producers build them."""
    if lg in ("nba", "wnba"):
        season = 2026 if lg == "nba" else 2025
        rows = nba.nba_possession_rate_rows(
            pl.read_parquet(RATES / f"{lg}_possessions_{season}.parquet"),
            pl.read_parquet(RATES / f"{lg}_schedule_{season}.parquet"),
            league_id="00" if lg == "nba" else "10",
        )
        return rows, None
    rows = mbb.ncaa_possession_rate_rows(
        pl.read_parquet(RATES / f"ncaa_{lg}_possessions_2025.parquet"),
        pl.read_parquet(RATES / f"{lg}_schedule_2025.parquet"),
        league="mens" if lg == "mbb" else "womens",
    )
    return rows, mbb.ncaa_team_box_count_rows(pl.read_parquet(RATES / f"ncaa_{lg}_team_box_2025.parquet"))


def _books(lg: str) -> pl.DataFrame:
    """The public wrapper's output, its loaders pointed at the fixtures."""
    with pytest.MonkeyPatch.context() as mp:
        if lg == "nba":
            mp.setattr(nba, "load_nba_stats_possessions", _load("nba_possessions_2026.parquet"))
            mp.setattr(nba, "load_nba_stats_schedules", _load("nba_schedule_2026.parquet"))
            return nba.nba_team_game_four_factors(2026)
        if lg == "wnba":
            from sportsdataverse.wnba import wnba_loaders
            from sportsdataverse.wnba.wnba_team_season_rates import wnba_team_game_four_factors

            mp.setattr(wnba_loaders, "load_wnba_stats_possessions", _load("wnba_possessions_2025.parquet"))
            mp.setattr(wnba_loaders, "load_wnba_stats_schedules", _load("wnba_schedule_2025.parquet"))
            return wnba_team_game_four_factors(2025)
        if lg == "mbb":
            mp.setattr(mbb, "load_ncaa_mbb_possessions", _load("ncaa_mbb_possessions_2025.parquet"))
            mp.setattr(mbb, "load_ncaa_mbb_team_box", _load("ncaa_mbb_team_box_2025.parquet"))
            mp.setattr(mbb, "load_mbb_schedule", _load("mbb_schedule_2025.parquet"))
            return mbb.mbb_team_game_four_factors(2025)
        from sportsdataverse.wbb import wbb_loaders
        from sportsdataverse.wbb.wbb_team_season_rates import wbb_team_game_four_factors

        mp.setattr(wbb_loaders, "load_ncaa_wbb_possessions", _load("ncaa_wbb_possessions_2025.parquet"))
        mp.setattr(wbb_loaders, "load_ncaa_wbb_team_box", _load("ncaa_wbb_team_box_2025.parquet"))
        mp.setattr(wbb_loaders, "load_wbb_schedule", _load("wbb_schedule_2025.parquet"))
        return wbb_team_game_four_factors(2025)


@pytest.fixture(scope="module", params=LEAGUES)
def league(request: pytest.FixtureRequest) -> tuple[str, pl.DataFrame]:
    return request.param, _books(request.param)


def test_one_row_per_team_game_with_the_possession_counts(league: tuple[str, pl.DataFrame]) -> None:
    lg, books = league
    assert dict(books.schema) == TEAM_GAME_FOUR_FACTORS_SCHEMA
    assert books.select("season", "team_id", "game_id").is_duplicated().sum() == 0
    rows, counts = _inputs(lg)
    games = season_counts(rows, counts=counts, by_game=True).filter(split="all")
    assert books.height == games.height
    joined = books.join(games, on=["season", "team_id", "game_id"], how="inner", validate="1:1", suffix="_g")
    assert joined.height == books.height
    assert (joined["poss"] == joined["poss_g"]).all() and (joined["pts"] == joined["pts_g"]).all()
    # every fixture game has its counts (the college ones have their team-box rows)
    assert books["shooting_pts"].null_count() == 0


def test_the_factors_add_up_to_points_against_a_league_average_offense(league: tuple[str, pl.DataFrame]) -> None:
    """The gate: on every real game, the five parts are pts - lg_ppp * poss, to float precision."""
    _, books = league
    total = books.select(pl.sum_horizontal(*FACTORS, "residual_pts")).to_series()
    assert (total - books["pts_vs_exp"]).abs().max() < 1e-9
    assert (books["exp_pts"] - books["lg_ppp"] * books["poss"]).abs().max() < 1e-9
    assert (books["pts_vs_exp"] - (books["pts"] - books["exp_pts"])).abs().max() < 1e-9
    # the league is its own average: points against it sum to zero, and so does each part
    for c in ("pts_vs_exp", *FACTORS, "residual_pts"):
        assert abs(books[c].sum()) < 1e-6, c
    # and the factors carry the game: the residual is the small part
    assert books["residual_pts"].abs().mean() < 0.25 * books["pts_vs_exp"].abs().mean()


def test_the_residual_is_the_possession_gap_exactly(league: tuple[str, pl.DataFrame]) -> None:
    """residual = (pts - box_pts - FGA * lg gap) + chance * (box_poss - poss - FGA * lg gap)."""
    lg, books = league
    rows, counts = _inputs(lg)
    games = season_counts(rows, counts=counts, by_game=True).filter(split="all")
    o = {k: pl.col(f"o_{k}") for k in ("fga", "fgm", "fg3m", "fta", "ftm", "oreb", "tov")}
    gaps = games.with_columns(
        (pl.col("pts") - (2 * o["fgm"] + o["fg3m"] + o["ftm"])).alias("pts_gap"),
        (o["fga"] - o["oreb"] + o["tov"] + FT_POSSESSION_WEIGHT * o["fta"] - pl.col("poss")).alias("poss_gap"),
        o["fga"].alias("fga"),
    )
    lg_gap = gaps.select(
        (pl.col("pts_gap").sum() / pl.col("fga").sum()).alias("pts"),
        (pl.col("poss_gap").sum() / pl.col("fga").sum()).alias("poss"),
    ).row(0, named=True)
    exact = gaps.join(books, on=["season", "team_id", "game_id"], validate="1:1").with_columns(
        (
            pl.col("pts_gap")
            - pl.col("fga") * lg_gap["pts"]
            + pl.col("chance_pts") * (pl.col("poss_gap") - pl.col("fga") * lg_gap["poss"])
        ).alias("formula")
    )
    assert (exact["formula"] - exact["residual_pts"]).abs().max() < 1e-9
    if lg in ("nba", "wnba"):  # the pros' points are the box's: only the possession gap is left
        assert (gaps["pts_gap"] == 0).all()


def test_home_minus_away_is_the_margin_against_the_expected_margin(league: tuple[str, pl.DataFrame]) -> None:
    lg, books = league
    pair = books.join(books, on=["season", "game_id"], suffix="_o").filter(pl.col("team_id") != pl.col("team_id_o"))
    if lg in ("mbb", "wbb"):  # a non-D1 opponent has no row: only the games with both sides pair
        assert pair.height > 0.8 * books.height
    else:
        assert pair.height == books.height
    parts = [*FACTORS, "residual_pts"]
    mine = (
        pair.select(pl.sum_horizontal(*parts)).to_series()
        - pair.select(pl.sum_horizontal(*[f"{c}_o" for c in parts])).to_series()
    )
    margin = (pair["pts"] - pair["pts_o"]) - pair["lg_ppp"] * (pair["poss"] - pair["poss_o"])
    assert (mine - margin).abs().max() < 1e-9


def test_pro_points_against_the_final_score() -> None:
    """The counted points are the possessions'; a final score above them is the points no possession
    carries (technical free throws), the only distance from the box score's margin."""
    books = _books("nba")
    sched = pl.read_parquet(RATES / "nba_schedule_2026.parquet").select(
        pl.col("game_id").cast(pl.Utf8), pl.col("home_team_id").cast(pl.Int64), "home_pts", "away_pts"
    )
    assert books.schema["game_id"] == sched.schema["game_id"]
    j = books.join(sched, on="game_id", how="inner", validate="m:1").with_columns(
        (
            pl.when(pl.col("team_id") == pl.col("home_team_id")).then("home_pts").otherwise("away_pts") - pl.col("pts")
        ).alias("gap")
    )
    assert j.height == books.height
    # never below the score; 1-2 points on about one team-game in nine (6 at most in 2025-26)
    assert j["gap"].min() >= 0 and j["gap"].quantile(0.99) <= 2
    assert (j["gap"] == 0).mean() > 0.85


def test_one_real_game_priced_by_hand() -> None:
    """A Celtics game recomputed from the raw possession capture, independently of season_counts."""
    books = _books("nba")
    raw = nba.nba_possession_rate_rows(
        pl.read_parquet(RATES / "nba_possessions_2026.parquet"), pl.read_parquet(RATES / "nba_schedule_2026.parquet")
    )
    team = 1610612738
    game = books.filter(team_id=team).sort("game_id")["game_id"][0]
    mine = raw.filter((pl.col("game_id") == game) & (pl.col("offense_team_id") == team))
    n, off, lg = mine.height, mine.sum(), raw.sum()
    keys = ("fga", "fgm", "fg3m", "fta", "ftm", "oreb", "dreb", "tov", "points")
    t = {k: off[k][0] for k in keys}
    L = {k: lg[k][0] for k in keys}  # noqa: N806
    lg_poss = raw.height
    ppp, tov = L["points"] / lg_poss, L["tov"] / lg_poss
    efg, orb = (L["fgm"] + 0.5 * L["fg3m"]) / L["fga"], L["oreb"] / (L["oreb"] + L["dreb"])
    ftr, ftp = L["fta"] / L["fga"], L["ftm"] / L["fta"]
    v = ppp / (1 - tov)
    rc = t["oreb"] + t["dreb"]
    want = {
        "shooting_pts": 2 * (t["fgm"] + 0.5 * t["fg3m"])
        - 2 * efg * t["fga"]
        + v * orb * (rc - (L["oreb"] + L["dreb"]) / L["fga"] * t["fga"]),
        "turnover_pts": (tov * n - t["tov"]) * v,
        "rebounding_pts": (t["oreb"] - orb * rc) * v,
        "free_throw_pts": (t["fta"] - ftr * t["fga"]) * (ftp - 0.44 * v) + (t["ftm"] - ftp * t["fta"]),
        "pts_vs_exp": t["points"] - ppp * n,
    }
    got = books.filter((pl.col("team_id") == team) & (pl.col("game_id") == game)).row(0, named=True)
    for k, w in want.items():
        assert got[k] == pytest.approx(w, abs=1e-9), k
    assert got["lg_ft_pct"] == pytest.approx(ftp) and got["chance_pts"] == pytest.approx(v)


def test_league_ft_pct_on_the_season_table() -> None:
    """Q21: lg_ft_pct is every team's FTM over every team's FTA, per season and split."""
    rows, _ = _inputs("nba")
    from sportsdataverse._common.team_season_rates import team_season_rates

    rates = team_season_rates(rows, minutes_per_game=48, level="nba")
    per_split = rates.group_by("split").agg(pl.col("lg_ft_pct").n_unique().alias("k"), pl.col("lg_ft_pct").first())
    assert (per_split["k"] == 1).all()
    whole = rates.filter(split="all")["lg_ft_pct"][0]
    assert whole == pytest.approx(rows["ftm"].sum() / rows["fta"].sum(), abs=1e-12)
    assert whole == pytest.approx(_books("nba")["lg_ft_pct"][0], abs=1e-12)
    # college: the team box carries free throws for every split but clutch, where it has no counts
    crows, counts = _inputs("mbb")
    college = team_season_rates(crows, minutes_per_game=40, counts=counts, level="mbb")
    assert college.filter(split="clutch")["lg_ft_pct"].null_count() == college.filter(split="clutch").height
    assert 0.6 < college.filter(split="all")["lg_ft_pct"][0] < 0.8


def test_empty_and_missing() -> None:
    empty = team_game_four_factors(pl.DataFrame(schema=RATE_ROWS_SCHEMA))
    assert dict(empty.schema) == TEAM_GAME_FOUR_FACTORS_SCHEMA and empty.height == 0
    with pytest.raises(KeyError, match="ftm"):
        team_game_four_factors(pl.DataFrame(schema=RATE_ROWS_SCHEMA).drop("ftm"))


def test_a_league_without_free_throws_is_null_never_nan() -> None:
    """A zero league denominator (the WNBA capture with its free throws zeroed) gives null prices."""
    rows, _ = _inputs("wnba")
    books = team_game_four_factors(rows.with_columns(pl.lit(0.0).alias("fta"), pl.lit(0.0).alias("ftm")))
    assert books["lg_ft_pct"].null_count() == books.height
    assert books["free_throw_pts"].null_count() == books["residual_pts"].null_count() == books.height
    assert not books.select(pl.col(pl.Float64).is_nan().any()).row(0, named=False).count(True)
    assert books["shooting_pts"].null_count() == 0  # the other factors are still priced
