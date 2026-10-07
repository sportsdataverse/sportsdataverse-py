"""The R-dictionary description fallback is scoped to the league's OWN sport.

Regression guard for the cross-sport ``_merged`` leak: the retired union of every SDV R
package put baseballr's "Inning number." on the stats.nba.com jersey-number column, cfbfastR's
SP+ text on an NFL Pro passer rating, wehoop's ARGUMENT text ("Whether to include statistical
ranks ...") on a WNBA leaderboard rank, and nflfastR's per-play "Binary indicator ... sack" on
NFL Pro season totals. Each case is pinned below on the resolver, and the generated docs are
swept for the phrases on pages of the wrong sport.
"""

from __future__ import annotations

import pathlib
import re

import pytest

import tools.codegen.generate as gen

DOCS = pathlib.Path(gen.ROOT) / "docs" / "docs"

# Phrase that must never render -> the league directories it legitimately belongs to.
_DENYLIST: dict[str, tuple[str, ...]] = {
    "Inning number": ("mlb", "baseball"),
    "SP+ rating": ("cfb",),
    "Bill Connelly": ("cfb",),
    "Whether to include statistical ranks": (),  # argument text: nowhere
    "Whether to return": (),
}

# (league, column, schema) -> text the resolver must NOT return (substring) / must return.
_SPOT_CASES = [
    ("nba", "num", "commonteamroster", "Inning"),
    ("wnba", "num", "commonteamroster", "Inning"),
    ("wnba", "rank", "leagueleaders", "Whether to include"),
    ("nfl", "rating", "players_offense_passing_season", "SP+"),
    ("nfl", "int", "defense_overview_season", "Binary"),
    ("nfl", "sack", "defense_overview_season", "play ended in a sack"),
    ("cfb", "rating", "player_all_rankings", "SP+"),  # On3 renders under cfb but is not cfbfastR
]


@pytest.mark.parametrize(("league", "col", "schema", "bad"), _SPOT_CASES)
def test_resolver_never_returns_wrong_sport_text(league, col, schema, bad):
    text = gen._r_col_desc(league, col, schema)
    assert bad not in text, f"{league}.{schema}.{col} resolved cross-sport text: {text!r}"


def test_own_sport_text_still_resolves():
    # cfbfastR's own column on a cfb page keeps its text; hoopR text reaches an NBA column.
    assert "SP+" in gen._r_col_desc("cfb", "rating")
    assert gen._r_col_desc("nba", "athlete_id") == gen._r_col_desc("mbb", "athlete_id") != ""
    # wehoop is hoopR's sibling: a column only wehoop documents still reaches an nba page ...
    wehoop_only = next(
        c
        for c, d in gen._r_col_descs()["wehoop"].items()
        if c not in gen._r_col_descs()["hoopR"] and d and not gen._R_ARGUMENT_TEXT.match(d)
    )
    assert gen._r_col_desc("nba", wehoop_only) != ""
    # ... but a baseballr-only column does not.
    assert gen._r_col_desc("nba", "num") == ""


def test_argument_text_is_dropped_everywhere():
    assert gen._R_ARGUMENT_TEXT.match(gen._r_col_descs()["wehoop"]["rank"])
    for league in ("wnba", "wbb", "nba", "mbb"):
        assert "Whether to include" not in gen._r_col_desc(league, "rank")


def test_aggregate_families_get_no_r_fallback():
    assert "players_offense_passing_season" in gen._no_r_fallback_schemas()
    assert gen._r_col_desc("nfl", "cpoe", "players_offense_passing_season") == ""
    assert gen._r_col_desc("nfl", "cpoe") != ""  # the same column on a play-by-play table keeps nflreadr's text


def test_no_cross_sport_merged_lookup():
    assert not hasattr(gen, "_sport_merged")
    assert "_merged" not in [p for p in gen._R_SIBLING_PACKAGES] and all(
        "_merged" not in sibs for sibs in gen._R_SIBLING_PACKAGES.values()
    )


def _league_dir(path: pathlib.Path) -> str:
    return path.relative_to(DOCS).parts[0]


@pytest.mark.skipif(not DOCS.exists(), reason="generated docs absent")
def test_generated_docs_carry_no_wrong_sport_text():
    hits = []
    for md in DOCS.rglob("reference/**/*.md"):
        text = md.read_text(encoding="utf-8")
        league = _league_dir(md)
        for phrase, allowed in _DENYLIST.items():
            if phrase in text and league not in allowed:
                hits.append(f"{md.relative_to(DOCS)}: {phrase!r}")
    # NFL Pro season tables: per-play "Binary ..." text must not describe a season total.
    for md in (DOCS / "nfl" / "reference" / "nflpro").glob("*.md"):
        if re.search(r"Binary (flag|indicator)", md.read_text(encoding="utf-8")):
            hits.append(f"{md.relative_to(DOCS)}: per-play 'Binary ...' text on a season table")
    assert not hits, "cross-sport description text in generated docs:\n" + "\n".join(hits[:20])
