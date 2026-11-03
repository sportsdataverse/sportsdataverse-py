"""Basketball id map (BB-D14) on real captured crosswalks.

Fixtures (provenance in each directory's README):

* ``tests/fixtures/crosswalk_basketball/``: the R producers' committed 2026 NBA / WNBA crosswalks.
* ``tests/fixtures/crosswalk_basketball_archive/``: 2015 NBA Finals games 5-6, stats.nba.com box scores and
  ESPN schedule / game rosters, run through the archive builder (the 0.92 Jaro-Winkler player match).
* ``tests/fixtures/basketball_release_ids/``: the 2025 WNBA Stats rosters (``player_id`` Int64) and the
  ``ncaa_mbb_team_box`` release rows.
* The NCAA <-> ESPN team crosswalk bundled with sdv-py (``sportsdataverse/{mbb,wbb}/data``).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.basketball.sources import idmap as m
from sportsdataverse.errors import NoDataError
from sportsdataverse.mbb.mbb_ncaa_espn_crosswalk import _packaged_crosswalk
from sportsdataverse.nba import nba_crosswalks_from_archive

FIX = Path(__file__).resolve().parents[1] / "fixtures"
XW = FIX / "crosswalk_basketball"
ARCHIVE = FIX / "crosswalk_basketball_archive"
RELEASE = FIX / "basketball_release_ids"


def _wnba(**kw) -> pl.DataFrame:
    kw.setdefault("players", pl.read_parquet(XW / "wnba_player_crosswalk_2026.parquet"))
    return m._build_idmap(
        "wnba",
        games=pl.read_parquet(XW / "wnba_schedule_crosswalk_2026.parquet"),
        teams=pl.read_parquet(XW / "wnba_team_crosswalk_2026.parquet"),
        **kw,
    )


def _stats_roster_2025() -> pl.DataFrame:
    return pl.read_parquet(RELEASE / "wnba_stats_rosters_2025.parquet").select(
        "season", pl.col("player_id").alias("source_id"), pl.col("player").alias("name")
    )


def _counts(idmap: pl.DataFrame, kind: str) -> tuple[int, int, int]:
    rows = idmap.filter(pl.col("kind") == kind)
    return rows.height, rows["source_id"].null_count(), rows["espn_id"].null_count()


@pytest.fixture(scope="module")
def nba_2015() -> pl.DataFrame:
    out = nba_crosswalks_from_archive(
        2015,
        pl.read_parquet(ARCHIVE / "espn" / "nba_schedule_2015.parquet"),
        pl.read_parquet(ARCHIVE / "espn" / "nba_game_rosters_2015.parquet"),
        ARCHIVE / "nba_stats" / "json",
    )
    return m._build_idmap("nba", games=out["schedule"], teams=out["team"], players=out["player"])


def test_schema_is_utf8_ids_and_one_source_per_league(nba_2015: pl.DataFrame) -> None:
    assert dict(nba_2015.schema) == m.IDMAP_SCHEMA
    assert nba_2015["source"].unique().to_list() == ["stats"]
    assert m.SOURCE == {"nba": "stats", "wnba": "stats", "mbb": "ncaa", "wbb": "ncaa"}


def test_nba_stats_ids_map_to_espn_and_back(nba_2015: pl.DataFrame) -> None:
    # Finals game 5: stats / cdn live 0041400405 <-> ESPN 400796849.
    game = m._nba_idmap("0041400405", frm="source", idmap=nba_2015)
    assert game["espn_id"] == "400796849" and game["match_method"] == "both"
    assert m._nba_idmap(400796849, idmap=nba_2015)["source_id"] == "0041400405"
    # Cleveland: ESPN 5 <-> 1610612739; LeBron James: ESPN 1966 <-> person id 2544 (0.92 rule, exact name).
    assert m._nba_idmap(5, kind="team", idmap=nba_2015)["source_id"] == "1610612739"
    lebron = m._nba_idmap(2544, kind="player", frm="source", idmap=nba_2015)
    assert (lebron["espn_id"], lebron["espn_team_id"], lebron["name"]) == ("1966", "5", "LeBron James")
    assert m._nba_idmap(1, kind="player", idmap=nba_2015) is None  # not in the map at all
    assert _counts(nba_2015, "player") == (27, 0, 0)


def test_unmatched_espn_athletes_stay_rows() -> None:
    xw = pl.read_parquet(XW / "nba_player_crosswalk_2026.parquet")
    idmap = m._build_idmap("nba", teams=pl.read_parquet(XW / "nba_team_crosswalk_2026.parquet"), players=xw)
    # 544 ESPN athletes in, 544 rows out: 395 paired, 149 kept with a null source id.
    assert _counts(idmap, "player") == (544, 149, 0)
    assert _counts(idmap, "team") == (30, 0, 0)
    unmatched = idmap.filter((pl.col("kind") == "player") & (pl.col("match_method") == "unmatched"))
    assert unmatched.height == 149 and unmatched["source_id"].null_count() == 149
    row = m._nba_idmap(unmatched["espn_id"][0], kind="player", idmap=idmap)
    assert row is not None and row["source_id"] is None  # "seen, unmatched", not "unknown"


def test_wnba_games_keep_both_sides_of_an_unpaired_game() -> None:
    idmap = _wnba()
    # 343 paired, 6 ESPN-only and 6 Stats-only (preseason games against clubs outside the league).
    assert _counts(idmap, "game") == (355, 6, 6)
    row = m._wnba_idmap("1012600012", frm="source", idmap=idmap)
    assert row["espn_id"] is None and row["match_method"] == "unmatched"


def test_source_players_no_crosswalk_row_claims_become_rows() -> None:
    idmap = _wnba(source_players=_stats_roster_2025())
    # 23 players on the 2025 Finals rosters; 20 are in the 2026 crosswalk, 3 are not.
    assert _counts(idmap, "player") == (206, 0, 3)
    left = idmap.filter(pl.col("espn_id").is_null() & (pl.col("kind") == "player"))
    assert left["match_method"].unique().to_list() == ["unmatched"]
    assert left["season"].unique().to_list() == [2025]


def test_float_origin_ids_never_become_dot_zero() -> None:
    roster = _stats_roster_2025()
    base = _wnba(source_players=roster)
    # A pandas round trip with a null turns integer ids into floats: 1630149 -> 1630149.0.
    floated = _wnba(
        players=pl.read_parquet(XW / "wnba_player_crosswalk_2026.parquet").with_columns(
            pl.col("espn_team_id").cast(pl.Float64)
        ),
        source_players=roster.with_columns(pl.col("source_id").cast(pl.Float64)),
    )
    assert floated.drop("built_at").sort(pl.all()).equals(base.drop("built_at").sort(pl.all()))
    assert not floated["source_id"].drop_nulls().str.ends_with(".0").any()
    assert m._wnba_idmap(1630149.0, kind="player", frm="source", idmap=floated) is not None


def test_an_already_stringified_float_id_raises() -> None:
    teams = pl.read_parquet(XW / "nba_team_crosswalk_2026.parquet")
    bad = teams.with_columns(pl.col("nba_team_id").cast(pl.Int64).cast(pl.Float64).cast(pl.Utf8))
    assert bad["nba_team_id"][0].endswith(".0")
    with pytest.raises(ValueError, match="float-origin"):
        m._build_idmap("nba", teams=bad)


def test_reviewed_overrides_win_and_retire_the_source_only_row() -> None:
    # Reviewed: same date and the same home team through the team leg (ESPN 6 = 1611661320, ESPN 17 =
    # 1611661319); the crosswalk could not pair them because the visitor is a non-WNBA club.
    overrides = pl.DataFrame(
        {"kind": ["game", "game"], "espn_id": ["401867790", "401869973"], "source_id": ["1012600012", "1012600017"]}
    )
    idmap = _wnba(overrides=overrides)
    assert _counts(idmap, "game") == (353, 4, 4)
    row = m._wnba_idmap("1012600012", frm="source", idmap=idmap)
    assert (row["espn_id"], row["match_method"], row["match_confidence"]) == ("401867790", "override", 1.0)


def test_an_override_that_lands_nowhere_keeps_the_source_only_row() -> None:
    # The ESPN id is not in the map, so no pair is written and the Stats-only row must stay.
    overrides = pl.DataFrame({"kind": ["game"], "espn_id": ["1"], "source_id": ["1012600012"]})
    row = m._wnba_idmap("1012600012", frm="source", idmap=_wnba(overrides=overrides))
    assert row is not None and row["espn_id"] is None and row["match_method"] == "unmatched"


def test_a_source_lookup_prefers_the_override_over_a_name_match() -> None:
    # Pin Stats game 1012600001 (crosswalked to ESPN 401866150) onto ESPN 401867790: both rows are now
    # paired in 2026, and the reviewed one must win the source -> ESPN lookup.
    overrides = pl.DataFrame({"kind": ["game"], "espn_id": ["401867790"], "source_id": ["1012600001"]})
    idmap = _wnba(overrides=overrides)
    assert idmap.filter(pl.col("source_id") == "1012600001").height == 2
    assert m._wnba_idmap("1012600001", frm="source", idmap=idmap)["espn_id"] == "401867790"


def test_college_team_leg_is_season_keyed_and_has_2026_27() -> None:
    for league, n_2027 in (("mbb", 365), ("wbb", 363)):
        idmap = m._build_idmap(league, teams=_packaged_crosswalk(league))
        assert idmap["source"].unique().to_list() == ["ncaa"]
        y27 = idmap.filter(pl.col("season") == 2027)  # "2026-27"
        assert y27.height == n_2027 and y27["espn_id"].null_count() == 0


def test_college_game_leg_from_the_ncaa_team_box() -> None:
    idmap = m._build_idmap("mbb", games=pl.read_parquet(RELEASE / "ncaa_mbb_team_box_2026.parquet"))
    assert _counts(idmap, "game") == (2, 0, 0)  # one row per team in, one per game out
    assert m._mbb_idmap(401804830, idmap=idmap)["source_id"] == "6475617"


def test_college_player_leg_is_a_documented_gap() -> None:
    players = pl.read_parquet(XW / "wbb_player_crosswalk_2026.parquet")
    with pytest.raises(ValueError, match="NCAA <-> ESPN player leg"):
        m._build_idmap("wbb", players=players)


def test_bad_arguments_raise() -> None:
    with pytest.raises(ValueError, match="unknown league"):
        m._build_idmap("nhl")
    with pytest.raises(ValueError, match="kind"):
        m._nba_idmap(1, kind="coach", idmap=pl.DataFrame(schema=m.IDMAP_SCHEMA))
    with pytest.raises(ValueError, match="not an id"):
        m._nba_idmap(1.5, idmap=pl.DataFrame(schema=m.IDMAP_SCHEMA))
    with pytest.raises(ValueError, match="base_url"):
        m._nba_idmap(1)


def test_fetch_idmap_row_reads_the_data_api_route() -> None:
    seen: dict = {}

    class _Resp:
        def __init__(self, body):
            self._body = body

        def json(self):
            return self._body

    def transport(url, **kw):
        seen.update(url=url, params=kw.get("params"))
        if url.endswith("/1"):
            raise NoDataError("404")
        return _Resp({"espn_id": "1966", "source_id": "2544"})

    row = m._nba_idmap(2544.0, kind="player", frm="source", base_url="https://api.example/", transport=transport)
    assert seen == {"url": "https://api.example/v1/nba/idmap/2544", "params": {"kind": "player", "from": "source"}}
    assert row["espn_id"] == "1966"
    assert m._fetch_idmap_row("nba", 1, base_url="https://api.example", transport=transport) is None


def test_a_source_id_an_override_displaces_stays_as_an_unmatched_row() -> None:
    # ESPN 401866150 is crosswalked to Stats 1012600001; reassigning it to 1012600012 must not lose 1012600001.
    overrides = pl.DataFrame({"kind": ["game"], "espn_id": ["401866150"], "source_id": ["1012600012"]})
    idmap = _wnba(overrides=overrides)
    assert m._wnba_idmap("401866150", idmap=idmap)["source_id"] == "1012600012"
    row = m._wnba_idmap("1012600001", frm="source", idmap=idmap)
    assert row is not None and row["espn_id"] is None and row["match_method"] == "unmatched"
    assert _counts(idmap, "game") == (355, 6, 6)  # 1012600012 left the source-only rows, 1012600001 took its place


def test_one_source_id_overridden_onto_two_espn_ids_raises() -> None:
    overrides = pl.DataFrame(
        {"kind": ["game", "game"], "espn_id": ["401867790", "401869973"], "source_id": ["1012600012", "1012600012"]}
    )
    with pytest.raises(ValueError, match="several espn_ids"):
        _wnba(overrides=overrides)


def test_one_espn_id_overridden_onto_two_source_ids_raises() -> None:
    overrides = pl.DataFrame(
        {"kind": ["game", "game"], "espn_id": ["401867790", "401867790"], "source_id": ["1012600012", "1012600017"]}
    )
    with pytest.raises(ValueError, match="several source_ids"):
        _wnba(overrides=overrides)


@pytest.mark.parametrize("seasons", ["2026", np.int64(2026), 2026.0, [2026]])
def test_college_release_build_reads_any_season_spelling(monkeypatch: pytest.MonkeyPatch, seasons) -> None:
    import sportsdataverse.mbb as mbb

    asked: list = []
    box = pl.read_parquet(RELEASE / "ncaa_mbb_team_box_2026.parquet")
    monkeypatch.setattr(mbb, "ncaa_espn_team_crosswalk", lambda league: _packaged_crosswalk(league))
    monkeypatch.setattr(mbb, "load_ncaa_mbb_team_box", lambda s: asked.append(s) or box)
    idmap = m._build_idmap_from_release("mbb", seasons)
    assert asked == [[2026]]
    teams = idmap.filter(pl.col("kind") == "team")
    assert teams["season"].unique().to_list() == [2026] and teams.height == 365  # "2025-26"
