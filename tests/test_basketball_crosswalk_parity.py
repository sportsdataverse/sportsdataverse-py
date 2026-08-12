"""Parity gate for the basketball crosswalk port (design section 9.3).

The golden fixtures under ``tests/fixtures/crosswalk_basketball/`` are the R
producers' **committed outputs** -- the ``<lg>/crosswalk/parquet/*.parquet``
files published by ``hoopR-mbb-data``, ``hoopR-nba-data``,
``wehoop-wbb-data`` and ``wehoop-wnba-data`` for the 2026 season. They are
copied unmodified; provenance lives in that directory's ``README.md``.

**How the gate works.** The live providers drift daily, so re-running the
network builders can never reproduce a fixed R output. Instead each test
*reconstructs the provider input frames out of the golden output itself* --
the golden carries every source-side field it consumed (``espn_*``, ``fox_*``,
``bart_*``, ``kp_*``, ``nba_*``, ``wnba_*``) -- feeds them to the ported pure
assembler, and asserts the assembler reproduces the golden **exactly**: every
join key, every ``match_method``, every ``*_match_confidence``, in the R
column order and dtypes. A 97%-right crosswalk fails.

What this does and does not cover:

* Covered completely -- the normalizers, all five alias tables, the greedy
  blocked matcher's assignment order, the Jaro-Winkler scores (compared to
  ~1e-9), the ``match_method`` case ladders, column order and dtypes. The two
  schedule crosswalks are a *complete* reconstruction: both the ESPN and the
  Torvik/Stats side survive in the golden as ``espn_only`` / ``bart_only`` /
  ``stats_only`` rows, so nothing is lost.
* Not covered -- provider rows that never matched anything are absent from an
  ESPN-anchored golden, so the team and player tests cannot prove a *negative*
  (that an unmatched row stayed unmatched because the key genuinely differs
  rather than because the candidate was missing). They do prove every match
  happened for the right reason, since a mistranscribed alias would misalign
  the key and drop the row.
* ``espn_birth_date`` is not published in the player goldens, so the NBA/WNBA
  DOB tiebreak is exercised only through the jersey tiebreak that precedes it.

**Where the golden is wrong.** The MBB team golden's ``bart_*`` columns are
all-null -- the R builder swallowed a Torvik outage in a ``tryCatch`` the day
it was frozen. Reconstruction-from-golden therefore hands the MBB assembler an
empty Torvik frame, which cannot distinguish a working join from a broken one.
:func:`test_mbb_team_crosswalk_joins_torvik_where_the_golden_froze_an_outage`
feeds the committed Torvik capture instead and pins the corrected output; the
golden is not the authority on that column.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.mbb import mbb_crosswalk
from sportsdataverse.nba import nba_crosswalk
from sportsdataverse.wbb import wbb_crosswalk
from sportsdataverse.wnba import wnba_crosswalk

FUZZY_FLOOR = 0.92
# Unmatched rows whose best *rejected* score the ESPN-anchored golden cannot
# carry (the losing provider players are not published). Measured, not chosen.
UNRECOVERABLE = {"wbb": 27, "mbb": 27, "wnba": 0, "nba": 149}
# ESPN MBB teams the committed Torvik capture (and the bundled KenPom
# directory) both carry, of 362. Measured, not chosen.
BART_JOINED = 359
FIXTURES = Path(__file__).parent / "fixtures" / "crosswalk_basketball"
SEASON = 2026


def golden(name: str) -> pl.DataFrame:
    """Load a committed R-producer crosswalk output."""
    path = FIXTURES / f"{name}_crosswalk_{SEASON}.parquet"
    if not path.exists():  # pragma: no cover - fixtures are committed
        pytest.skip(f"missing golden fixture {path.name}")
    return pl.read_parquet(path)


def same(got: pl.DataFrame, want: pl.DataFrame, keys: list[str]) -> None:
    """Assert row-level equality after sorting both sides on ``keys``."""
    assert got.columns == want.columns, f"column order drift: {got.columns} != {want.columns}"
    assert got.height == want.height, f"row-count drift: {got.height} != {want.height}"
    assert_frame_equal(
        got.sort(keys, nulls_last=True),
        want.sort(keys, nulls_last=True),
        check_row_order=True,
        check_dtypes=True,
        check_exact=False,
        rel_tol=1e-9,
        abs_tol=1e-12,
    )


def same_players(got: pl.DataFrame, want: pl.DataFrame, keys: list[str]) -> int:
    """Player-crosswalk equality, isolating the one unreconstructable column.

    Every column including ``match_method`` must be exact. ``match_confidence``
    must also be exact on every row that matched. On an **unmatched** row, R
    stores the best *rejected* Jaro-Winkler score -- computed against provider
    players who matched nobody, and those players appear nowhere in an
    ESPN-anchored golden, so the reconstruction cannot supply them. With a
    reduced candidate pool the assembler may legitimately score a different
    leftover or none at all; what must still hold on both sides is that the
    row is genuinely unmatched, i.e. neither score reaches
    :data:`FUZZY_FLOOR`. Each caller then pins the exact count, so a change in
    how many rows land here is itself a failure.

    Returns:
        The number of unmatched rows whose rejected-best score the fixture
        cannot carry.
    """
    assert got.columns == want.columns, f"column order drift: {got.columns} != {want.columns}"
    assert got.height == want.height, f"row-count drift: {got.height} != {want.height}"
    g = got.sort(keys, nulls_last=True)
    w = want.sort(keys, nulls_last=True)

    others = [c for c in w.columns if c != "match_confidence"]
    assert_frame_equal(g.select(others), w.select(others), check_row_order=True, check_dtypes=True)

    matched = w["match_method"] != "unmatched"
    assert_frame_equal(
        g.filter(matched).select("match_confidence"),
        w.filter(matched).select("match_confidence"),
        check_exact=False,
        rel_tol=1e-9,
        abs_tol=1e-12,
    )

    unrecoverable = 0
    for mine, theirs in zip(
        g.filter(~matched)["match_confidence"].to_list(), w.filter(~matched)["match_confidence"].to_list()
    ):
        if mine == theirs:
            continue
        # On an unmatched row the score describes the best *rejected*
        # candidate, so it is a function of the provider players who matched
        # nobody -- rows an ESPN-anchored golden never publishes. With a
        # reduced pool the assembler may legitimately score a different
        # leftover, or none at all. What must still hold on both sides is that
        # the row is genuinely unmatched: neither score reaches the floor.
        assert mine is None or mine < FUZZY_FLOOR, f"rejected score reached the floor: {mine}"
        assert theirs is None or theirs < FUZZY_FLOOR, f"golden rejected score reached the floor: {theirs}"
        unrecoverable += 1
    return unrecoverable


def espn_from(g: pl.DataFrame) -> pl.DataFrame:
    """Rebuild the ESPN team directory the R builder consumed."""
    out = g.unique(subset=["espn_team_id"], keep="first", maintain_order=True).select(
        pl.col("espn_team_id").alias("team_id"),
        pl.col("espn_abbreviation").alias("abbreviation"),
        pl.col("espn_display_name").alias("display_name"),
        pl.col("espn_short_name").alias("short_name"),
        pl.col("espn_location").alias("team"),
        pl.col("espn_mascot").alias("mascot"),
    )
    if "espn_conference" in g.columns:
        out = out.with_columns(
            g.unique(subset=["espn_team_id"], keep="first", maintain_order=True)["espn_conference"].alias(
                "conference_name"
            )
        )
    return out


def distinct_where(g: pl.DataFrame, flag: str, mapping: dict[str, str]) -> pl.DataFrame:
    """Rebuild one provider's frame from the matched rows of a golden."""
    return (
        g.filter(pl.col(flag).is_not_null())
        .select([pl.col(src).alias(dst) for src, dst in mapping.items()])
        .unique(keep="first", maintain_order=True)
    )


# ---------------------------------------------------------------------------
# Team crosswalks
# ---------------------------------------------------------------------------


def test_wbb_team_crosswalk_parity() -> None:
    want = golden("wbb_team")
    got = wbb_crosswalk._assemble_team_crosswalk(
        espn_from(want),
        distinct_where(
            want,
            "fox_team_id",
            {"fox_team_id": "fox_team_id", "fox_team_name": "fox_team_name", "fox_section": "fox_section"},
        ),
        distinct_where(want, "bart_team", {"bart_team": "team", "bart_conf": "conf"}),
        SEASON,
    )
    same(got, want, ["espn_team_id"])


def test_mbb_team_crosswalk_parity() -> None:
    want = golden("mbb_team")
    got = mbb_crosswalk._assemble_team_crosswalk(
        espn_from(want),
        distinct_where(
            want,
            "fox_team_id",
            {"fox_team_id": "fox_team_id", "fox_team_name": "fox_team_name", "fox_section": "fox_section"},
        ),
        distinct_where(want, "bart_team", {"bart_team": "team", "bart_conf": "conf"}),
        distinct_where(want, "kp_team", {"kp_team": "Team", "kp_conf": "Conf"}),
        SEASON,
    )
    same(got, want, ["espn_team_id"])


def test_mbb_team_crosswalk_bundled_kenpom_reproduces_the_golden() -> None:
    """The **bundled** KenPom directory is the R producer's KenPom input.

    :func:`test_mbb_team_crosswalk_parity` reconstructs KenPom out of the
    golden, so it proves the matcher but says nothing about
    ``sportsdataverse/mbb/data/kp_team_info.csv`` -- a stale or wrong bundle
    would sail through it. This feeds the real bundled directory instead
    (hoopR's ``teams_links``, filtered to the season by
    :func:`~sportsdataverse.mbb.mbb_crosswalk._kenpom_teams`) and asserts the
    assembler still reproduces the golden exactly, ``kp_team`` / ``kp_conf`` /
    ``kp_match_confidence`` / ``match_method`` included.

    Torvik is passed in empty on purpose: the 2026 golden's ``bart_*`` columns
    are all-null because the R builder's ``torvik_ratings()`` call returned
    nothing the day it was frozen (it is wrapped in
    ``tryCatch(torvik_ratings(year = season), error = function(e) NULL)``),
    so an empty frame is what reproduces that golden. It is not a claim that
    Torvik is unavailable -- see
    :func:`test_mbb_team_crosswalk_joins_torvik_where_the_golden_froze_an_outage`,
    which feeds the real captured Torvik directory and is what pins the
    *correct* behaviour. This test deliberately does not.
    """
    want = golden("mbb_team")
    got = mbb_crosswalk._assemble_team_crosswalk(
        espn_from(want),
        distinct_where(
            want,
            "fox_team_id",
            {"fox_team_id": "fox_team_id", "fox_team_name": "fox_team_name", "fox_section": "fox_section"},
        ),
        pl.DataFrame(),
        mbb_crosswalk._kenpom_teams(SEASON),
        SEASON,
    )
    same(got, want, ["espn_team_id"])
    assert got["kp_team"].is_not_null().sum() == 359


def test_mbb_team_crosswalk_joins_torvik_where_the_golden_froze_an_outage() -> None:
    """Python populating ``bart_*`` is the PASSING state, not a parity break.

    The 2026 MBB golden's ``bart_*`` columns are all-null and its
    ``match_method`` reads ``fox+kp``, because the R builder wraps its Torvik
    fetch in ``tryCatch(torvik_ratings(year = season), error = function(e)
    NULL)`` (``hoopR/R/mbb_crosswalk.R:346-347``) and that fetch failed the day
    the fixture was frozen. The golden therefore records a **transient
    upstream outage, not intended behaviour** -- reproducing it would be a
    regression. The Python builder has no such swallow (a Torvik failure
    raises ``CrosswalkSourceError``), so it joins Torvik for real.

    Every other test in this file reconstructs the provider inputs out of the
    golden, which for MBB Torvik yields an empty frame -- so none of them can
    tell a working Torvik join from a broken one. This one feeds the committed
    Torvik capture (``mbb_torvik_teams_2026.parquet``, provenance in the
    fixture README) and pins the divergence in both directions: the golden's
    defect and Python's corrected output.
    """
    want = golden("mbb_team")
    bart = pl.read_parquet(FIXTURES / f"mbb_torvik_teams_{SEASON}.parquet")

    # The golden's recorded defect -- every Torvik-sourced column, not just
    # ``bart_team``: a partially populated golden is no longer a clean record
    # of the outage and must not be used as the reference. If a re-capture ever
    # fixes it, this fails and the whole test should collapse back into the
    # plain parity gate.
    for column in ("bart_team", "bart_conf", "bart_match_confidence"):
        assert want[column].is_null().all(), f"{column} is not all-null in the frozen golden"
    assert want.filter(pl.col("match_method") == "fox+kp").height == BART_JOINED

    got = mbb_crosswalk._assemble_team_crosswalk(
        espn_from(want),
        distinct_where(
            want,
            "fox_team_id",
            {"fox_team_id": "fox_team_id", "fox_team_name": "fox_team_name", "fox_section": "fox_section"},
        ),
        bart,
        mbb_crosswalk._kenpom_teams(SEASON),
        SEASON,
    )

    # Floor on the populated rate + Torvik's participation in match_method.
    # Measured against the committed capture, not chosen.
    assert got["bart_team"].is_not_null().sum() == BART_JOINED
    assert got["bart_conf"].is_not_null().sum() == BART_JOINED
    assert got["bart_match_confidence"].is_not_null().sum() == BART_JOINED
    assert got.filter(pl.col("match_method") == "fox+bart+kp").height == BART_JOINED
    assert got.filter(pl.col("match_method") == "fox+kp").height == 0
    # The three ESPN teams Torvik's directory genuinely does not carry.
    assert sorted(got.filter(pl.col("bart_team").is_null())["espn_location"].to_list()) == [
        "LSU New Orleans",
        "St. Thomas",
        "West Florida",
    ]

    # ...and nothing else moved: strip Torvik back out and the golden returns,
    # column order, dtypes and every other match_method included.
    diverging = ["bart_team", "bart_conf", "bart_match_confidence", "match_method"]
    same(got.drop(diverging), want.drop(diverging), ["espn_team_id"])
    assert (
        got.sort("espn_team_id")["match_method"].str.replace("+bart", "", literal=True).to_list()
        == want.sort("espn_team_id")["match_method"].to_list()
    )


def test_kenpom_teams_falls_back_to_the_newest_bundled_year() -> None:
    """Out-of-range seasons reuse the newest capture (``mbb_crosswalk.R:349-354``)."""
    newest = mbb_crosswalk._kenpom_teams(2026)
    assert newest.height == 365
    assert mbb_crosswalk._kenpom_teams(2002).height == 327
    assert mbb_crosswalk._kenpom_teams(1999).to_dicts() == newest.to_dicts()
    assert mbb_crosswalk._kenpom_teams(2030).to_dicts() == newest.to_dicts()


def test_wnba_team_crosswalk_parity() -> None:
    want = golden("wnba_team")
    got = wnba_crosswalk._assemble_team_crosswalk(
        espn_from(want),
        distinct_where(
            want,
            "wnba_team_id",
            {
                "wnba_team_id": "wnba_team_id",
                "wnba_team_tricode": "wnba_team_tricode",
                "wnba_team_name": "wnba_team_name",
                "wnba_team_city": "wnba_team_city",
                "wnba_team_slug": "wnba_team_slug",
            },
        ),
        distinct_where(want, "fox_team_id", {"fox_team_id": "fox_team_id", "fox_team_name": "fox_team_name"}),
        SEASON,
    )
    same(got, want, ["espn_team_id", "wnba_team_id"])


def test_nba_team_crosswalk_parity() -> None:
    want = golden("nba_team")
    got = nba_crosswalk._assemble_team_crosswalk(
        espn_from(want),
        distinct_where(
            want,
            "nba_team_id",
            {
                "espn_team_id": "espn_team_id",
                "nba_team_id": "nba_team_id",
                "nba_team_abbreviation": "nba_team_abbreviation",
                "nba_team_name": "nba_team_name",
                "nba_team_city": "nba_team_city",
                "nba_team_slug": "nba_team_slug",
                "nba_conference": "nba_conference",
                "nba_division": "nba_division",
            },
        ),
        distinct_where(want, "fox_team_id", {"fox_team_id": "fox_team_id", "fox_team_name": "fox_team_name"}),
        SEASON,
    )
    same(got, want, ["espn_team_id", "nba_team_id"])


# ---------------------------------------------------------------------------
# Player crosswalks
# ---------------------------------------------------------------------------

_ESPN_PLAYER = {
    "espn_team_id": "espn_team_id",
    "team_abbreviation": "team_abbreviation",
    "espn_athlete_id": "espn_athlete_id",
    "espn_full_name": "espn_full_name",
    "espn_jersey": "espn_jersey",
    "espn_position": "espn_position",
}
_FOX_PLAYER = {
    "espn_team_id": "espn_team_id",
    "fox_athlete_id": "fox_athlete_id",
    "fox_player": "fox_player",
    "fox_jersey": "fox_jersey",
    "fox_position_group": "fox_position_group",
}


def _player_espn_fox(want: pl.DataFrame, assemble, **kwargs) -> pl.DataFrame:
    frames = []
    for (team_id,), block in want.group_by(["espn_team_id"], maintain_order=True):
        espn = block.select([pl.col(src).alias(dst) for src, dst in _ESPN_PLAYER.items()])
        fox = distinct_where(block, "fox_athlete_id", _FOX_PLAYER)
        frames.append(assemble(espn, fox, SEASON, **kwargs))
        assert team_id is not None
    return pl.concat(frames, how="diagonal_relaxed")


def test_wbb_player_crosswalk_parity() -> None:
    from sportsdataverse._common_crosswalk_basketball import assemble_player_espn_fox

    want = golden("wbb_player")
    got = _player_espn_fox(want, assemble_player_espn_fox)
    lost = same_players(got, want, ["espn_team_id", "espn_athlete_id"])
    assert lost == UNRECOVERABLE["wbb"], f"unreconstructable rejected-best rows moved: {lost}"


def test_mbb_player_crosswalk_parity() -> None:
    from sportsdataverse._common_crosswalk_basketball import assemble_player_espn_fox

    want = golden("mbb_player")
    got = _player_espn_fox(want, assemble_player_espn_fox, exact_tiebreak=True)
    lost = same_players(got, want, ["espn_team_id", "espn_athlete_id"])
    assert lost == UNRECOVERABLE["mbb"], f"unreconstructable rejected-best rows moved: {lost}"


def _player_espn_stats_fox(want: pl.DataFrame, prefix: str, *, exact_tiebreak: bool) -> pl.DataFrame:
    from sportsdataverse._common_crosswalk_basketball import assemble_player_espn_stats_fox

    stats_map = {
        "espn_team_id": "espn_team_id",
        f"{prefix}_player_id": f"{prefix}_player_id",
        f"{prefix}_player_name": f"{prefix}_player_name",
        f"{prefix}_jersey_num": f"{prefix}_jersey_num",
        f"{prefix}_position": f"{prefix}_position",
    }
    frames = []
    for _, block in want.group_by(["espn_team_id"], maintain_order=True):
        espn = block.select([pl.col(src).alias(dst) for src, dst in _ESPN_PLAYER.items()]).with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("espn_birth_date")
        )
        stats = distinct_where(block, f"{prefix}_player_id", stats_map).with_columns(
            pl.lit(None, dtype=pl.Utf8).alias(f"{prefix}_birth_date")
        )
        fox = distinct_where(block, "fox_athlete_id", _FOX_PLAYER)
        frames.append(assemble_player_espn_stats_fox(espn, stats, fox, SEASON, prefix, exact_tiebreak=exact_tiebreak))
    return pl.concat(frames, how="diagonal_relaxed")


def test_wnba_player_crosswalk_parity() -> None:
    want = golden("wnba_player")
    got = _player_espn_stats_fox(want, "wnba", exact_tiebreak=False)
    lost = same_players(got, want, ["espn_team_id", "espn_athlete_id"])
    assert lost == UNRECOVERABLE["wnba"], f"unreconstructable rejected-best rows moved: {lost}"


def test_nba_player_crosswalk_parity() -> None:
    want = golden("nba_player")
    got = _player_espn_stats_fox(want, "nba", exact_tiebreak=True)
    lost = same_players(got, want, ["espn_team_id", "espn_athlete_id"])
    assert lost == UNRECOVERABLE["nba"], f"unreconstructable rejected-best rows moved: {lost}"


# ---------------------------------------------------------------------------
# Schedule crosswalks (complete reconstruction: both sides survive the golden)
# ---------------------------------------------------------------------------


def test_wbb_schedule_crosswalk_parity() -> None:
    want = golden("wbb_schedule")
    espn_games = want.filter(pl.col("espn_game_id").is_not_null()).select(
        "espn_game_id", "game_date", "home_espn_team_id", "away_espn_team_id"
    )
    bart_games = want.filter(pl.col("bart_muid").is_not_null()).select(
        pl.col("bart_muid").alias("muid"),
        pl.col("game_date"),
        pl.col("bart_team1").alias("team1"),
        pl.col("bart_team2").alias("team2"),
        pl.col("bart_winner").alias("winner"),
    )
    got = wbb_crosswalk._assemble_schedule_crosswalk(espn_games, bart_games, golden("wbb_team"), SEASON)
    same(got, want, ["espn_game_id", "bart_muid"])


def test_wnba_schedule_crosswalk_parity() -> None:
    want = golden("wnba_schedule")
    espn_games = want.filter(pl.col("espn_game_id").is_not_null()).select(
        "espn_game_id", "game_date", "home_espn_team_id", "away_espn_team_id"
    )
    stats_games = want.filter(pl.col("wnba_game_id").is_not_null()).select(
        "game_date",
        "season_type",
        "wnba_game_id",
        "wnba_game_code",
        "wnba_home_team_id",
        "wnba_away_team_id",
    )
    got = wnba_crosswalk._assemble_schedule_crosswalk(espn_games, stats_games, golden("wnba_team"), SEASON)
    same(got, want, ["espn_game_id", "wnba_game_id"])


# ---------------------------------------------------------------------------
# Alias tables are transcriptions, not derivations
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        # Values produced by R: stringdist::stringsim(a, b, method = "jw", p = 0.1).
        # The first four are real crosswalk pairs with an ODD transposition
        # count -- they are the only reason a `// 2` scorer bug is visible, and
        # every textbook Jaro-Winkler example has an even count.
        ("jana mbambo njoya", "jana mbombo njoya", 0.9327205882352941),
        ("nadechka keka laccen", "nadechka laccen", 0.9299999999999999),
        ("goundoba diakite bayo", "goundo diakite bayo", 0.9230576441102757),
        ("travis pourciau", "travis porciau", 0.9509523809523809),
        # Canonical stringdist examples (even transposition counts).
        ("dwayne", "duane", 0.8400000000000000),
        ("martha", "marhta", 0.9611111111111111),
        ("dixon", "dicksonx", 0.8133333333333332),
    ],
)
def test_jaro_winkler_matches_r_stringdist(left: str, right: str, expected: float) -> None:
    from sportsdataverse._common_crosswalk_basketball import jaro_winkler

    assert jaro_winkler(left, right) == pytest.approx(expected, abs=1e-15)


def test_alias_tables_only_merge_spellings() -> None:
    """No alias may map a name onto another alias's key (that would chain)."""
    for table in (
        wbb_crosswalk.BART_ALIAS,
        wbb_crosswalk.FOX_DISPLAY_ALIAS,
        mbb_crosswalk.BART_ALIAS,
        mbb_crosswalk.KP_ALIAS,
        mbb_crosswalk.FOX_DISPLAY_ALIAS,
    ):
        chained = {k: v for k, v in table.items() if v in table and table[v] != v}
        assert not chained, f"alias chain would change resolution order: {chained}"


def test_kp_alias_extends_bart_alias() -> None:
    """KenPom reuses every Torvik alias plus exactly three of its own."""
    extra = {k: v for k, v in mbb_crosswalk.KP_ALIAS.items() if k not in mbb_crosswalk.BART_ALIAS}
    assert extra == {
        "CSUN": "Cal State Northridge",
        "SIUE": "SIU Edwardsville",
        "Southeast Missouri": "Southeast Missouri State",
    }
    shared = {k: v for k, v in mbb_crosswalk.KP_ALIAS.items() if k in mbb_crosswalk.BART_ALIAS}
    assert shared == mbb_crosswalk.BART_ALIAS
