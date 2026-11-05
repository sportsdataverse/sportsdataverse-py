"""BB-D8: NBA / WNBA coach seasons and careers on committed real captures.

Fixtures (``tests/fixtures/bb_coach_seasons/README.md``; none synthetic): the stats.nba.com
schedules and coach staffs, ESPN's coach records and the ESPN-to-stats team crosswalk for NBA
2016-2026 and WNBA 2016-2025, and Basketball-Reference's per-coach game counts for the same
seasons (the public oracle). BB-D3's 2023-24 / 2025-26 NBA possessions give the ratings.

Gates are the observed values (never loosened to pass). The oracle is placed with the same
:func:`coach_of_record` from its own counts, so a game is scored against the coach BBRef credits.
"""

from __future__ import annotations

import importlib
import logging
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common.coach_seasons import (
    COACH_CAREERS_SCHEMA,
    COACH_SEASONS_SCHEMA,
    STINTS_SCHEMA,
    coach_careers,
    coach_of_record,
    coach_seasons,
)
from sportsdataverse._common.team_season_rates import team_season_rates

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
FIX = FIXTURES / "bb_coach_seasons"
RATES = FIXTURES / "bb_team_season_rates"
mod = importlib.import_module("sportsdataverse.nba.nba_coach_seasons")
tsr = importlib.import_module("sportsdataverse.nba.nba_team_season_rates")
#: stats.nba.com abbreviation -> Basketball-Reference's, per league.
_BBREF_ABBR = {"nba": {"BKN": "BRK", "CHA": "CHO", "PHX": "PHO"}, "wnba": {"SAN": "SAS", "PHX": "PHO"}}

#: (league, ESPN counts used) -> (games BBRef places, credited of them, credited to BBRef's coach).
GATES = {
    ("nba", True): (25926, 19967, 19799),
    ("nba", False): (25926, 17935, 17772),
    ("wnba", True): (4108, 2246, 2246),
    ("wnba", False): (4108, 1772, 1772),
}
#: league -> (our coach-seasons, named in BBRef, same games and wins, one game apart). Counts only:
#: nothing on the BBRef side goes through coach_of_record.
COUNT_GATES = {"nba": (260, 257, 250, 4), "wnba": (63, 63, 61, 2)}


def _name(col: str) -> pl.Expr:
    return pl.col(col).str.to_lowercase().str.replace_all(r"[^a-z]", "")


def _games(lg: str) -> pl.DataFrame:
    return mod.nba_team_games(pl.read_parquet(FIX / f"{lg}_schedule.parquet"))


def _stints(lg: str, *, espn: bool) -> pl.DataFrame:
    games = _games(lg)
    records = (
        pl.read_parquet(FIX / f"{lg}_espn_coach_records.parquet")
        if espn
        else pl.DataFrame(schema=mod.ESPN_RECORDS_SCHEMA)
    )
    heads = mod.head_coaches(pl.read_parquet(FIX / f"{lg}_stats_coaches.parquet"))
    stints = mod.nba_coach_stints(heads, records, pl.read_parquet(FIX / f"{lg}_team_crosswalk.parquet"))
    return stints.filter(pl.col("season").is_in(games["season"].unique().implode()))


def _oracle(lg: str, games: pl.DataFrame) -> pl.DataFrame:
    teams = games.with_columns(pl.col("team").replace(_BBREF_ABBR[lg])).select("season", "team_id", "team").unique()
    bbref = pl.read_parquet(FIX / "bbref_coach_records.parquet").filter(pl.col("league") == lg)
    assert bbref.join(teams, on=["season", "team"], how="anti").height == 0  # every BBRef team maps
    counts = (
        bbref.join(teams, on=["season", "team"], how="inner")
        .with_columns(pl.int_range(pl.len()).cast(pl.Int64).alias("coach_id"))
        .select(
            "season",
            "team_id",
            "coach_id",
            "coach",
            pl.col("g").alias("games"),
            pl.col("w").alias("wins"),
            pl.lit(None, dtype=pl.Utf8).alias("position"),
        )
    )
    return coach_of_record(games, counts.cast(STINTS_SCHEMA))


@pytest.mark.parametrize(("lg", "espn"), list(GATES))
def test_coach_of_record_against_basketball_reference(lg: str, espn: bool) -> None:
    games = _games(lg)
    credit = coach_of_record(games, _stints(lg, espn=espn))
    scored = _oracle(lg, games).join(credit, on=["game_id", "team_id"], how="left", suffix="_ours")
    placed, credited, right = GATES[(lg, espn)]
    assert scored.height == placed
    assert scored["coach_ours"].is_not_null().sum() == credited
    assert scored.select((_name("coach") == _name("coach_ours")).sum()).item() == right


@pytest.mark.parametrize("lg", list(COUNT_GATES))
def test_coach_season_counts_against_basketball_reference(lg: str) -> None:
    games = _games(lg)
    credit = coach_of_record(games, _stints(lg, espn=True))
    ours = (
        games.join(credit, on=["game_id", "team_id"], how="inner")
        .with_columns(pl.col("team").replace(_BBREF_ABBR[lg]))
        .group_by("season", "team", "coach")
        .agg(pl.len().alias("games"), (pl.col("pts") > pl.col("opp_pts")).sum().alias("wins"))
        .with_columns(_name("coach").alias("key"))
    )
    bbref = pl.read_parquet(FIX / "bbref_coach_records.parquet").filter(pl.col("league") == lg)
    j = ours.join(bbref.with_columns(_name("coach").alias("key")), on=["season", "team", "key"], how="left")
    n, named, exact, one_off = COUNT_GATES[lg]
    assert j.height == n
    assert j["g"].is_not_null().sum() == named
    assert j.filter((pl.col("games") == pl.col("g")) & (pl.col("wins") == pl.col("w"))).height == exact
    assert j.filter((pl.col("games") - pl.col("g")).abs() == 1).height == one_off


def test_uncredited_team_seasons_are_logged(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        coach_of_record(_games("nba"), _stints("nba", espn=True))
    # an opener and a closer who differ with no ESPN count can be an in-season or an offseason change
    assert "team-seasons uncredited (no single placement fits)" in caplog.text
    okc = _games("nba").filter((pl.col("season") == 2020) & (pl.col("team") == "OKC"))
    # ESPN counts Mark Daigneault (then an assistant) 4 games under the 2019-20 Thunder; as the
    # closer they must be the last 4, which do not hold his 1 win: the season stays uncredited
    assert coach_of_record(okc, _stints("nba", espn=True)).height == 0


def test_espn_counts_only_for_a_named_candidate() -> None:
    # ESPN lists Darko Rajakovic (then a Thunder assistant) with 2019 Thunder records: never a stint
    records = pl.read_parquet(FIX / "nba_espn_coach_records.parquet")
    assert records.filter((pl.col("season") == 2019) & (pl.col("coach") == "Darko Rajakovic")).height > 0
    stints = _stints("nba", espn=True)
    assert set(stints.filter(pl.col("coach") == "Darko Rajakovic")["team_id"]) == {1610612761}  # Toronto only
    counted = stints.filter(pl.col("games").is_not_null())
    assert counted.height > 100 and set(counted["position"]) <= {"first", "last", "any"}
    # a closer is a stint only with his own count: never a count-less gap filler
    assert stints.filter(pl.col("games").is_null() & (pl.col("position") == "last")).height == 0


def test_mid_season_change_is_split_by_game() -> None:
    games = _games("nba")
    credit = coach_of_record(games, _stints("nba", espn=True))
    cle = (
        games.filter((pl.col("season") == 2016) & (pl.col("team") == "CLE"))
        .join(credit, on=["game_id", "team_id"], how="inner")
        .sort("game_date")
    )
    # David Blatt 30-11, then Tyronn Lue 27-14 (Basketball-Reference): ESPN counts Blatt's 41
    assert cle["coach"].to_list() == ["David Blatt"] * 41 + ["Tyronn Lue"] * 41
    won = cle["pts"] > cle["opp_pts"]
    assert (won.head(41).sum(), won.tail(41).sum()) == (30, 27)


def test_unplaceable_team_season_is_left_uncredited(caplog: pytest.LogCaptureFixture) -> None:
    games = _games("nba").filter((pl.col("season") == 2016) & (pl.col("team") == "CLE"))
    real = _stints("nba", espn=True).filter((pl.col("season") == 2016) & (pl.col("team_id") == games["team_id"][0]))
    assert coach_of_record(games, real).height == 82
    off_by_one = real.with_columns(pl.col("wins") + 1)  # no run of 41 games holds 31 wins at either end
    with caplog.at_level(logging.WARNING):
        assert coach_of_record(games, off_by_one).height == 0
    assert "uncredited" in caplog.text
    too_many = real.with_columns(pl.when(pl.col("games").is_not_null()).then(pl.lit(83)).otherwise(None).alias("games"))
    assert coach_of_record(games, too_many).height == 0
    # positions are binding: ESPN counts Lue (41 games, 27 wins), the closer; marked as the opener,
    # his run cannot sit at the end where those wins are, so nothing is credited
    lue_opens = real.with_columns(
        pl.when(pl.col("games").is_not_null()).then(pl.lit("first")).otherwise(pl.lit("any")).alias("position")
    )
    assert real.filter(pl.col("games").is_not_null())["position"].to_list() == ["last"]
    assert coach_of_record(games, lue_opens).height == 0


def _rows(season: int) -> pl.DataFrame:
    sched = pl.read_parquet(FIX / "nba_schedule.parquet").filter(pl.col("season") == season)
    return tsr.nba_possession_rate_rows(
        pl.read_parquet(RATES / f"nba_possessions_{season}.parquet"), sched, league_id="00"
    )


@pytest.fixture(scope="module")
def seasons_2024_2026() -> pl.DataFrame:
    games = _games("nba").filter(pl.col("season").is_in([2024, 2026]))
    credit = coach_of_record(games, _stints("nba", espn=True))
    rows = pl.concat([_rows(2024), _rows(2026)])
    return coach_seasons(games, credit, rows, minutes_per_game=48)


def test_coach_rates_add_up_to_the_team(seasons_2024_2026: pl.DataFrame) -> None:
    out = seasons_2024_2026
    assert dict(out.schema) == COACH_SEASONS_SCHEMA
    team = team_season_rates(_rows(2024), minutes_per_game=48).filter(split="all")
    was = out.filter((pl.col("season") == 2024) & (pl.col("team") == "WAS")).sort("first_game_date")
    assert was["coach"].to_list() == ["Wes Unseld", "Brian Keefe"]
    assert was["games"].to_list() == [42, 40] and was["games"].sum() == was["team_games"][0] == 82
    t = team.filter(pl.col("team_id") == was["team_id"][0]).row(0, named=True)
    assert (was["poss"].sum(), was["opp_poss"].sum(), was["poss_pts"].sum()) == (t["poss"], t["opp_poss"], t["pts"])
    # a coach who had the whole season has the team's rates
    full = out.filter((pl.col("season") == 2024) & (pl.col("games") == pl.col("team_games")))
    joined = full.join(team, on="team_id", suffix="_team")
    assert joined.height == full.height > 15
    for m in ("off_rtg", "def_rtg", "net_rtg", "pace", "efg_pct", "opp_tov_pct"):
        assert joined[m].null_count() == joined[f"{m}_team"].null_count() == 0, m
        assert (joined[m] - joined[f"{m}_team"]).abs().max() < 1e-9, m
    assert out["net_rtg_rank"].drop_nulls().min() == 1.0
    assert out.filter(pl.col("games") < 41)["net_rtg_rank"].null_count() == out.filter(pl.col("games") < 41).height


def test_careers_pool_counts(seasons_2024_2026: pl.DataFrame) -> None:
    careers = coach_careers(seasons_2024_2026, minutes_per_game=48)
    assert dict(careers.schema) == COACH_CAREERS_SCHEMA
    assert careers["coach_id"].n_unique() == careers.height
    assert careers["games"].sum() == seasons_2024_2026["games"].sum()
    two = seasons_2024_2026.filter(pl.len().over("coach_id") == 2)  # coached in both seasons
    assert two["coach_id"].n_unique() > 10
    cid = two["coach_id"][0]
    rows, career = two.filter(pl.col("coach_id") == cid), careers.filter(pl.col("coach_id") == cid).row(0, named=True)
    assert career["seasons"] == 2 and (career["first_season"], career["last_season"]) == (2024, 2026)
    assert career["off_rtg"] == pytest.approx(100 * rows["poss_pts"].sum() / rows["poss"].sum(), rel=1e-12)
    assert career["win_pct"] == pytest.approx(rows["wins"].sum() / rows["games"].sum(), rel=1e-12)
    # careers rank only coaches with a full season of games
    assert (
        careers.filter(pl.col("games") < 82)["win_pct_rank"].null_count() == careers.filter(pl.col("games") < 82).height
    )


def test_playoffs_go_to_the_last_regular_season_coach() -> None:
    sched = pl.read_parquet(FIX / "nba_schedule.parquet")
    regular = mod.nba_team_games(sched)
    credit = coach_of_record(regular, _stints("nba", espn=True))
    post = mod.nba_team_games(sched, season_type="playoffs")
    carried = mod.carry_last_coach(post, regular, credit)
    bos = carried.join(post.select("game_id", "team_id", "team", "season"), on=["game_id", "team_id", "season"]).filter(
        (pl.col("season") == 2024) & (pl.col("team") == "BOS")
    )
    assert bos.height == 19 and set(bos["coach"]) == {"Joe Mazzulla"}
    # a team whose regular season is uncredited (Milwaukee: three coaches in 2023-24) gets no playoff rows
    assert (
        carried.join(
            post.filter((pl.col("season") == 2024) & (pl.col("team") == "MIL")), on=["game_id", "team_id"]
        ).height
        == 0
    )
    # Sacramento 2024-25 is credited only for Mike Brown's first 32 games: its last game is not, so
    # nothing carries (Doug Christie, not Brown, coached the end of that season)
    sac = regular.filter((pl.col("season") == 2025) & (pl.col("team") == "SAC"))
    assert credit.join(sac, on=["game_id", "team_id"]).height == 32
    assert mod.carry_last_coach(sac.sort("game_date").tail(1), regular, credit).height == 0


def test_nba_coach_seasons_end_to_end(monkeypatch: pytest.MonkeyPatch) -> None:
    loaders = importlib.import_module("sportsdataverse.nba.nba_loaders")
    sched = pl.read_parquet(FIX / "nba_schedule.parquet")
    staff = pl.read_parquet(FIX / "nba_stats_coaches.parquet")
    calls: dict[str, list] = {}

    def fixture(name: str, frame: pl.DataFrame):  # type: ignore[no-untyped-def]
        def load(seasons):  # type: ignore[no-untyped-def]
            calls[name] = list(seasons)
            return frame.filter(pl.col("season").is_in([s + 1 for s in seasons]))

        return load

    monkeypatch.setattr(tsr, "load_nba_stats_schedules", fixture("schedule", sched))
    monkeypatch.setattr(
        tsr, "load_nba_stats_possessions", lambda s: pl.read_parquet(RATES / "nba_possessions_2024.parquet")
    )
    monkeypatch.setattr(loaders, "load_nba_stats_coaches", fixture("staff", staff))
    monkeypatch.setattr(
        loaders,
        "load_nba_team_crosswalk",
        lambda s: (
            pl.read_parquet(FIX / "nba_team_crosswalk.parquet")
            .filter(pl.col("season").is_in(s))
            .rename({"team_id": "nba_team_id"})
        ),
    )
    monkeypatch.setattr(
        mod,
        "espn_coach_records",
        lambda s, league_id: pl.read_parquet(FIX / "nba_espn_coach_records.parquet").filter(pl.col("season") == s),
    )
    out = mod.nba_coach_seasons(2024)
    assert calls == {"schedule": [2023], "staff": [2022, 2023]}
    assert dict(out.schema) == COACH_SEASONS_SCHEMA and set(out["season"]) == {2024}
    # BBRef: Unseld 43 / Keefe 39; ESPN's count for Unseld is 42, and the split follows it
    was = out.filter(pl.col("team") == "WAS").sort("first_game_date")
    assert was["coach"].to_list() == ["Wes Unseld", "Brian Keefe"] and was["games"].to_list() == [42, 40]
    assert out["pbp_games"].sum() == out["games"].sum()
    with pytest.raises(ValueError, match="preseason"):
        mod.nba_coach_seasons(2024, season_type="preseason")
    calls.clear()
    mod.nba_coach_seasons(1997)  # 1996-97: the first staff file; there is none before it to read
    assert calls["staff"] == [1996]
