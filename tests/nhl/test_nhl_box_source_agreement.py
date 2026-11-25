"""The NHL api-web boxscore and the ESPN summary boxscore agree on every skater's line.

Two independent sources for the same three 2025-26 games (``tests/fixtures/nhl_box_agreement/``,
see its README). The sources share no game or player id, so:

* games pair through the fixture table below, and each pair is checked to name the same home and
  away teams (ESPN's full team name mapped to the NHL abbreviation by the package's own
  ``NHL_TEAM_FULLNAME_TO_ABBR``, because the abbreviations differ: ``TB`` / ``TBL``, ``SJ`` /
  ``SJS``, ``NJ`` / ``NJD``);
* skaters join on (game, team, sweater number), which is unique inside a team's game sheet, and
  each joined pair must also agree on the player's name, folded to (first initial, surname).
"""

import json
import unicodedata
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common_espn_parsers import parse_summary_boxscore_player
from sportsdataverse.nhl.nhl_api_web_parsers import _space_initial, parse_nhl_web_boxscore
from sportsdataverse.nhl.nhl_player_impact_constants import NHL_TEAM_FULLNAME_TO_ABBR

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nhl_box_agreement"
# (NHL api-web game id, ESPN event id): NYR @ BUF, NJD @ TBL, VGK @ SJS
GAMES = [(2025020009, 401802360), (2025020026, 401802377), (2025020020, 401802371)]
KEY = ["game", "team", "sweater_number"]
STATS = ["goals", "assists", "shots", "plus_minus"]


def _load(name: str) -> dict:
    return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


def _name_key(name: str) -> str:
    """``"Connor McDavid"`` and ``"C. McDavid"`` -> ``"c|mcdavid"``; ``"J.T. Miller"`` and ``"J. Miller"`` -> ``"j|miller"``."""
    folded = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower().replace(".", " ")
    words = [w for w in folded.split() if w not in ("jr", "sr", "ii", "iii")]
    return f"{words[0][0]}|{words[-1]}"


def _nhl_skaters(nhl_id: int) -> pl.DataFrame:
    payload = _load(f"nhl_boxscore_{nhl_id}")
    abbr = {"home": payload["homeTeam"]["abbrev"], "away": payload["awayTeam"]["abbrev"]}
    return parse_nhl_web_boxscore(payload).select(
        pl.lit(nhl_id).alias("game"),
        pl.col("home_away").replace_strict(abbr).alias("team"),
        pl.col("sweater_number").cast(pl.Int64),
        pl.col("name_default").alias("name"),
        *(pl.col(c).cast(pl.Int64).alias(s) for c, s in zip(["goals", "assists", "sog", "plus_minus"], STATS)),
    )


def _espn_skaters(nhl_id: int, espn_id: int) -> pl.DataFrame:
    return parse_summary_boxscore_player(_load(f"espn_summary_{espn_id}")).select(
        pl.lit(nhl_id).alias("game"),
        pl.col("team_display_name").replace_strict(NHL_TEAM_FULLNAME_TO_ABBR).alias("team"),
        pl.col("athlete_jersey").cast(pl.Int64).alias("sweater_number"),
        pl.col("athlete_display_name").alias("name"),
        *(pl.col(c).cast(pl.Int64).alias(s) for c, s in zip(["goals", "assists", "shots_total", "plus_minus"], STATS)),
    )


@pytest.fixture(scope="module")
def sources() -> tuple[pl.DataFrame, pl.DataFrame]:
    nhl = pl.concat([_nhl_skaters(n) for n, _ in GAMES])
    espn = pl.concat([_espn_skaters(n, e) for n, e in GAMES])
    return nhl, espn


@pytest.mark.parametrize(("nhl_id", "espn_id"), GAMES)
def test_each_game_pair_names_the_same_home_and_away_teams(nhl_id, espn_id):
    nhl = _load(f"nhl_boxscore_{nhl_id}")
    espn = {
        c["homeAway"]: c["team"]["abbreviation"]
        for c in _load(f"espn_summary_{espn_id}")["header"]["competitions"][0]["competitors"]
    }
    teams = {
        e["team"]["abbreviation"]: NHL_TEAM_FULLNAME_TO_ABBR[e["team"]["displayName"]]
        for e in _load(f"espn_summary_{espn_id}")["boxscore"]["players"]
    }
    assert (teams[espn["home"]], teams[espn["away"]]) == (nhl["homeTeam"]["abbrev"], nhl["awayTeam"]["abbrev"])


def test_every_skater_joins_one_to_one_across_sources(sources):
    nhl, espn = sources
    for k in KEY:
        assert nhl.schema[k] == espn.schema[k], k
    for frame in (nhl, espn):
        assert frame.select(KEY).is_duplicated().sum() == 0
    joined = nhl.join(espn, on=KEY, how="inner", suffix="_espn")
    assert nhl.height == espn.height == joined.height == 108  # 3 games x 2 teams x 18 skaters
    mismatched = joined.filter(
        pl.col("name").map_elements(_name_key, return_dtype=pl.Utf8)
        != pl.col("name_espn").map_elements(_name_key, return_dtype=pl.Utf8)
    )
    assert mismatched.is_empty(), mismatched.select("team", "sweater_number", "name", "name_espn").rows()


def test_goals_assists_shots_and_plus_minus_agree_on_every_skater(sources):
    nhl, espn = sources
    joined = nhl.join(espn, on=KEY, how="inner", suffix="_espn")
    for stat in STATS:
        # a null on either side would drop out of a `!=` filter, so it has to fail on its own
        assert joined[stat].null_count() == joined[f"{stat}_espn"].null_count() == 0, stat
        diff = joined.filter(pl.col(stat).ne_missing(pl.col(f"{stat}_espn")))
        assert diff.is_empty(), (stat, diff.select("team", "name", stat, f"{stat}_espn").rows())
    assert joined["goals"].sum() > 0 and joined["assists"].sum() > 0  # the games carry real scoring


def test_unspaced_initial_names_are_rewritten():
    names = _nhl_skaters(2025020009).filter(pl.col("team") == "BUF")["name"]
    assert "J. Kulich" in names.to_list()
    assert not names.str.contains(r"^\S+\.\S+$").any()
    assert _space_initial("J.Kulich") == "J. Kulich"
    assert _space_initial("J.T. Compher") == "J.T. Compher"
    assert _space_initial("C. McDavid") == "C. McDavid"
    assert _space_initial(None) is None
