"""As-of probe for `nba_player_props` (C-M10; ledger gate nba-props-asof-probe).

A projection for game G must be built only from what was known before G tipped off: changing G's own box
line must not move G's projection. Today the one-call surface reads the season's complete box logs, so it
did (the audit's probe: the game's own points 10 -> 50 moved `stat_pts_exp` 9.90 -> 19.80). Task 3.7 added
`as_of_date` (strict `<` on box lines AND the ratings snapshot, mirroring `nhl_player_props`), defaulting to the
target game's own date.

Inputs are the committed real 2023-24 corpus (`tests/fixtures/nba_prediction/`, README there): ESPN player
box logs (35,028 rows) with each game's date joined from `results_2024.parquet`, and the stats.nba.com
team ratings crosswalked to ESPN team ids (`pace` -> `adj_pace`). The loaders are monkeypatched; no network.
"""

from __future__ import annotations

import importlib
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.errors import AsOfCutWarning

mod = importlib.import_module("sportsdataverse.nba.nba_player_props")

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"


@pytest.fixture(scope="module")
def corpus():
    results = pl.read_parquet(FIX / "results_2024.parquet")
    logs = (
        pl.read_parquet(FIX / "player_box_logs_2024.parquet")
        .drop_nulls("minutes")
        .join(results.select("game_id", pl.col("date").alias("game_date")), on="game_id", how="left")
    )
    assert logs["game_date"].null_count() == 0
    ratings = pl.read_parquet(FIX / "team_ratings_oracle_2024.parquet").select(
        "team_id", pl.col("pace").alias("adj_pace")
    )
    # Target: the first game on/after 2024-01-15 (mid-season, so earlier AND later games exist), and its
    # home team's heaviest-minutes player in that game.
    game = results.filter(pl.col("date") >= date(2024, 1, 15)).sort("date", "game_id").row(0, named=True)
    player = (
        logs.filter((pl.col("game_id") == game["game_id"]) & (pl.col("team_id") == game["home_team_id"]))
        .sort("minutes", descending=True)
        .row(0, named=True)
    )
    return logs, ratings, game, player


def _schedule_date(game_id):
    """The game's date from the committed real schedule (`results_2024.parquet`); None if absent."""
    hit = pl.read_parquet(FIX / "results_2024.parquet").filter(pl.col("game_id") == str(game_id))
    return hit["date"][0] if hit.height else None


def _project(monkeypatch, logs, ratings, game, player_id, **kwargs):
    monkeypatch.setattr(mod, "_load_player_logs", lambda season, league_id: logs)
    monkeypatch.setattr(mod, "_load_game_date", lambda season, league_id, game_id: _schedule_date(game_id))
    monkeypatch.setattr(mod, "_load_ratings", lambda season, league_id, as_of_date=None: ratings)
    out = mod.nba_player_props(2024, game["game_id"], game["home_team_id"], game["away_team_id"], **kwargs)
    row = out.filter(pl.col("player_id") == player_id)
    assert row.height == 1
    return float(row["stat_pts_exp"][0])


def _bump(logs, game_id, player_id, by=40.0):
    hit = (pl.col("game_id") == game_id) & (pl.col("player_id") == player_id)
    assert logs.filter(hit).height == 1
    return logs.with_columns(pl.when(hit).then(pl.col("pts") + by).otherwise(pl.col("pts")).alias("pts"))


def test_probe_is_sensitive_to_an_earlier_game(monkeypatch, corpus):
    """Control: an EARLIER game's line must move the projection, so the as-of probe below is not vacuous."""
    logs, ratings, game, player = corpus
    earlier = (
        logs.filter((pl.col("player_id") == player["player_id"]) & (pl.col("game_date") < game["date"]))
        .sort("game_date")
        .row(0, named=True)
    )
    base = _project(monkeypatch, logs, ratings, game, player["player_id"])
    moved = _project(
        monkeypatch, _bump(logs, earlier["game_id"], player["player_id"]), ratings, game, player["player_id"]
    )
    assert moved > base + 0.1, (base, moved)
    print("NBA_PROPS_PROBE_SENSITIVE_OK", round(base, 3), round(moved, 3))


def test_own_line_does_not_move_its_projection(monkeypatch, corpus):
    logs, ratings, game, player = corpus
    base = _project(monkeypatch, logs, ratings, game, player["player_id"])
    bumped = _project(
        monkeypatch, _bump(logs, game["game_id"], player["player_id"]), ratings, game, player["player_id"]
    )
    assert bumped == pytest.approx(base, abs=1e-9), f"own +40 pts moved stat_pts_exp {base:.3f} -> {bumped:.3f}"


def test_a_later_line_does_not_move_its_projection(monkeypatch, corpus):
    """A fix that drops only the target game (game_id != G) but keeps every later game still leaks;
    the as-of filter the owner ruling mirrors from nhl_player_props is game_date < G's date."""
    logs, ratings, game, player = corpus
    later = (
        logs.filter((pl.col("player_id") == player["player_id"]) & (pl.col("game_date") > game["date"]))
        .sort("game_date")
        .row(0, named=True)
    )
    base = _project(monkeypatch, logs, ratings, game, player["player_id"])
    bumped = _project(
        monkeypatch, _bump(logs, later["game_id"], player["player_id"]), ratings, game, player["player_id"]
    )
    assert bumped == pytest.approx(base, abs=1e-9), f"a later +40 pts moved stat_pts_exp {base:.3f} -> {bumped:.3f}"


def test_default_cut_is_the_target_games_date_and_reaches_the_ratings(monkeypatch, corpus):
    """The default call cuts at G's own date, and the SAME strict cut goes to the ratings snapshot."""
    logs, ratings, game, player = corpus
    seen = {}

    def _ratings(season, league_id, as_of_date=None):
        seen["as_of_date"] = as_of_date
        return ratings

    monkeypatch.setattr(mod, "_load_player_logs", lambda season, league_id: logs)
    monkeypatch.setattr(mod, "_load_ratings", _ratings)
    default = mod.nba_player_props(2024, game["game_id"], game["home_team_id"], game["away_team_id"])
    assert seen["as_of_date"] == game["date"]
    explicit = mod.nba_player_props(
        2024, game["game_id"], game["home_team_id"], game["away_team_id"], as_of_date=game["date"]
    )
    assert default.equals(explicit)


def test_explicit_as_of_date_is_strict(monkeypatch, corpus):
    """A cut one day after G admits G's own line (so the default's exclusion is the strict `<`, not luck)."""
    logs, ratings, game, player = corpus
    after = game["date"] + timedelta(days=1)
    base = _project(monkeypatch, logs, ratings, game, player["player_id"], as_of_date=after)
    bumped = _project(
        monkeypatch,
        _bump(logs, game["game_id"], player["player_id"]),
        ratings,
        game,
        player["player_id"],
        as_of_date=after,
    )
    assert bumped > base + 0.1, (base, bumped)


def test_played_game_missing_from_the_logs_is_cut_at_its_schedule_date(monkeypatch, corpus):
    """R850-1: a played game absent from the box logs (an ESPN gap) still gets the as-of cut, from the
    schedule; without it a later line would leak into the projection."""
    logs, ratings, game, player = corpus
    gap = logs.filter(pl.col("game_id") != game["game_id"])
    later = (
        gap.filter((pl.col("player_id") == player["player_id"]) & (pl.col("game_date") > game["date"]))
        .sort("game_date")
        .row(0, named=True)
    )
    base = _project(monkeypatch, gap, ratings, game, player["player_id"])
    bumped = _project(
        monkeypatch, _bump(gap, later["game_id"], player["player_id"]), ratings, game, player["player_id"]
    )
    assert bumped == pytest.approx(base, abs=1e-9)
    assert base == pytest.approx(_project(monkeypatch, logs, ratings, game, player["player_id"]), abs=1e-9)


def test_game_in_neither_logs_nor_schedule_warns(monkeypatch, corpus):
    logs, ratings, game, player = corpus
    ghost = dict(game, game_id="999999999")
    with pytest.warns(AsOfCutWarning, match="game 999999999 .*no as-of cut"):
        _project(monkeypatch, logs, ratings, ghost, player["player_id"])
