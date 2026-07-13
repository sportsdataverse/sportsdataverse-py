"""Player + lineup play-context aggregations (CTG's on/off and lineup tables).

These roll the possession-level play-context frame up two more ways than
:func:`team_play_context` does:

* :func:`lineup_play_context` — one row per 5-man **offensive** lineup.
* :func:`player_play_context` — one row per player, with the team's offensive
  context **with them on the floor vs off it**, and the on-minus-off difference.
  This is the offensive half of CTG's player On/Off page.

Both consume a possession frame that has been through
:func:`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`, so they
need the ``off_player_1..5`` columns.

The load-bearing gate here is an **exact partition identity**, not a
correlation: for every player, ``on_poss + off_poss`` must equal their team's
total possessions. A player is either on the floor for a team possession or they
are not — there is no third bucket, so any leak (double-counted possession,
dropped null lineup slot, wrong team attribution) breaks the identity exactly.
Points partition the same way.
"""

from __future__ import annotations

import json
import pathlib

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away,
    parse_rotation_resultsets,
    players_on_court_from_rotation,
)
from sportsdataverse.nba.nba_play_context import (
    LINEUP_PLAY_CONTEXT_SCHEMA,
    PLAYER_PLAY_CONTEXT_SCHEMA,
    add_play_context,
    lineup_play_context,
    player_play_context,
    team_play_context,
)
from sportsdataverse.nba.nba_possessions import attach_possession_lineups

FXROOT = pathlib.Path("tests/fixtures/nba_engine")
GAMES = ["0022100001", "0022200001", "0022300001"]


def _ctx_with_lineups(game_id: str) -> pl.DataFrame:
    """Possessions + play context + the 5v5 lineup columns (offline, committed fixtures).

    ``attach_possession_lineups`` passes non-possession columns straight through,
    so the play-context enrichment and the lineup attach compose in either order;
    this is the natural one (enrich, then attach).
    """
    fx = FXROOT / game_id
    enh = enhanced_pbp_from_payload(json.loads((fx / "playbyplayv3.json").read_text()))
    home, away = boxscore_home_away(json.loads((fx / "boxscoretraditionalv3.json").read_text()))
    oncourt = players_on_court_from_rotation(
        enh,
        parse_rotation_resultsets(json.loads((fx / "gamerotation.json").read_text())),
        home_team_id=home,
        away_team_id=away,
    )
    return attach_possession_lineups(add_play_context(enh), oncourt, enh, home_team_id=home)


@pytest.fixture(scope="module")
def frames() -> dict[str, pl.DataFrame]:
    return {g: _ctx_with_lineups(g) for g in GAMES}


# ---------------------------------------------------------------------------
# Lineup table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", GAMES)
def test_lineup_schema_and_nonempty(frames, game_id: str) -> None:
    out = lineup_play_context(frames[game_id])
    assert out.height > 0
    for col, dtype in LINEUP_PLAY_CONTEXT_SCHEMA.items():
        assert col in out.columns, f"missing {col}"
        assert out.schema[col] == dtype, f"{col}: {out.schema[col]} != {dtype}"


@pytest.mark.parametrize("game_id", GAMES)
def test_lineup_possessions_partition_the_team(frames, game_id: str) -> None:
    """PARTITION IDENTITY: every team possession belongs to exactly one lineup."""
    ctx = frames[game_id]
    lineups = lineup_play_context(ctx, min_poss=0)
    team = team_play_context(ctx)

    by_team = lineups.group_by("offense_team_id").agg(
        pl.col("poss").sum().alias("poss"), pl.col("points").sum().alias("points")
    )
    joined = team.join(by_team, on="offense_team_id", how="inner", suffix="_lineups")
    assert joined.height == team.height >= 2, "lineup rollup lost a team"
    assert (joined["poss"] == joined["poss_lineups"]).all(), "lineup possessions do not partition the team's"
    assert (joined["points"] == joined["points_lineups"]).all(), "lineup points do not partition the team's"


def test_lineup_min_poss_filter(frames) -> None:
    ctx = frames[GAMES[0]]
    assert lineup_play_context(ctx, min_poss=10).height < lineup_play_context(ctx, min_poss=0).height
    assert (lineup_play_context(ctx, min_poss=10)["poss"] >= 10).all()


# ---------------------------------------------------------------------------
# Player on/off table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", GAMES)
def test_player_schema_and_nonempty(frames, game_id: str) -> None:
    out = player_play_context(frames[game_id])
    assert out.height > 0
    for col, dtype in PLAYER_PLAY_CONTEXT_SCHEMA.items():
        assert col in out.columns, f"missing {col}"
        assert out.schema[col] == dtype, f"{col}: {out.schema[col]} != {dtype}"


@pytest.mark.parametrize("game_id", GAMES)
def test_player_on_off_partitions_the_team(frames, game_id: str) -> None:
    """THE GATE. on_poss + off_poss == the player's team's possessions, exactly.

    A player is either on the floor for a team's offensive possession or they are
    not. Any leak — a possession counted for both, a null lineup slot silently
    dropped, a player attributed to the wrong team — breaks this identity. Points
    partition identically.
    """
    ctx = frames[game_id]
    players = player_play_context(ctx)
    team = team_play_context(ctx).select("offense_team_id", "poss", "points")

    joined = players.join(team, on="offense_team_id", how="inner", suffix="_team")
    assert joined.height == players.height, "a player is attributed to a team not in the team table"
    assert joined.height >= 16, f"only {joined.height} player rows — fixture shrunk?"

    assert (joined["on_poss"] + joined["off_poss"] == joined["poss"]).all(), (
        "on_poss + off_poss != team poss — the on/off split leaks"
    )
    assert (joined["on_points"] + joined["off_points"] == joined["points"]).all(), (
        "on_points + off_points != team points"
    )


@pytest.mark.parametrize("game_id", GAMES)
def test_player_diff_is_on_minus_off(frames, game_id: str) -> None:
    p = player_play_context(frames[game_id]).filter((pl.col("on_poss") > 0) & (pl.col("off_poss") > 0))
    assert p.height > 0
    got = p["diff_pts_per_100"].to_list()
    want = [a - b for a, b in zip(p["on_pts_per_100"].to_list(), p["off_pts_per_100"].to_list())]
    assert got == pytest.approx(want)


@pytest.mark.parametrize("game_id", GAMES)
def test_player_transition_freq_is_a_rate(frames, game_id: str) -> None:
    p = player_play_context(frames[game_id]).filter(pl.col("on_poss") > 0)
    assert ((p["on_transition_freq"] >= 0.0) & (p["on_transition_freq"] <= 1.0)).all()


# ---------------------------------------------------------------------------
# Contracts
# ---------------------------------------------------------------------------


def test_missing_lineup_columns_raises_not_silently_wrong(frames) -> None:
    """Without off_player_1..5 these tables are meaningless — fail loudly."""
    no_lineups = frames[GAMES[0]].drop([f"off_player_{i}" for i in range(1, 6)])
    with pytest.raises(ValueError, match="off_player"):
        lineup_play_context(no_lineups)
    with pytest.raises(ValueError, match="off_player"):
        player_play_context(no_lineups)


def test_empty_input_returns_documented_schema() -> None:
    empty = pl.DataFrame(schema={**{f"off_player_{i}": pl.Int64 for i in range(1, 6)}})
    for fn, schema in (
        (lineup_play_context, LINEUP_PLAY_CONTEXT_SCHEMA),
        (player_play_context, PLAYER_PLAY_CONTEXT_SCHEMA),
    ):
        out = fn(empty)
        assert out.height == 0
        assert list(out.columns) == list(schema)


def test_pandas_flag(frames) -> None:
    assert isinstance(lineup_play_context(frames[GAMES[0]], return_as_pandas=True), pd.DataFrame)
    assert isinstance(player_play_context(frames[GAMES[0]], return_as_pandas=True), pd.DataFrame)


def test_team_count_dtypes_match_the_empty_schema(frames) -> None:
    """REGRESSION. ``pl.len()`` / ``Boolean.sum()`` are UInt32, so before the explicit
    Int64 cast in ``_context_counts`` the team table returned UInt32 counts on real
    data while its zero-row branch declared Int64 — the same frame had two schemas
    depending on whether it had rows. The subtraction that derives the OFF side in
    ``player_play_context`` would also wrap on an unsigned type.
    """
    populated = team_play_context(frames[GAMES[0]])
    empty = team_play_context(frames[GAMES[0]].clear())
    for col in ("poss", "points", "transition_poss", "transition_points", "halfcourt_poss"):
        assert populated.schema[col] == pl.Int64, f"{col} is {populated.schema[col]}, want Int64"
        assert populated.schema[col] == empty.schema[col], f"{col}: populated/empty schema disagree"
