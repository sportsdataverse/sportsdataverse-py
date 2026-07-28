"""NBA per-game box logs + per-100 feature builder (SPM/BPM input substrate)."""

from __future__ import annotations

from typing import Callable, Dict, List, Optional

import polars as pl

from sportsdataverse.nba.nba_stats import nba_stats_leaguegamelog

# per-100 feature columns (snake-cased leaguegamelog player fields)
_STATS = ["pts", "fg3m", "fga", "fta", "ast", "oreb", "dreb", "stl", "blk", "tov", "pf"]

#: On-court players per team. A team-game's ``min`` sums all five slots, so
#: dividing it by this recovers the game's elapsed minutes -- the basis a
#: player's share of team possessions must be taken against.
ON_COURT_PLAYERS = 5.0


def box_features(
    player_logs: pl.DataFrame,
    team_logs: pl.DataFrame,
    *,
    game_ids: Optional[List[str]] = None,
) -> pl.DataFrame:
    """Aggregate per-player per-100-possession box features over a set of games.

    Restricting ``game_ids`` to a fold's games is the harness leakage guard.

    Per-100 possessions are computed per game (so mid-window trades use each
    game's own team pace), then summed — the result is fully deterministic.

    Args:
        player_logs: Per-player-per-game box lines (``game_id``, ``team_id``,
            ``player_id``, ``min``, and the counting stats in ``_STATS``).
        team_logs: Per-team-per-game lines (``game_id``, ``team_id``, ``min``,
            ``fga``, ``oreb``, ``tov``, ``fta``) for the possession estimate.
        game_ids: Optional subset of ``game_id`` to include (default: all).

    Returns:
        One row per player: ``player_id``, the ``_STATS`` per-100 rates, ``min``
        (total), ``gp`` (games). Empty frame with that schema on empty input.
    """
    if game_ids is not None:
        player_logs = player_logs.filter(pl.col("game_id").is_in(game_ids))
        team_logs = team_logs.filter(pl.col("game_id").is_in(game_ids))
    # Canonicalize real-parser column-name variants to the names ``_STATS`` expects:
    # ``leaguegamelog`` snake-cases ``FG3M`` -> ``"fg3_m"`` but our stats use ``"fg3m"``.
    # Backward-compatible: synthetic fixtures already use ``"fg3m"`` (no rename applied).
    if "fg3_m" in player_logs.columns and "fg3m" not in player_logs.columns:
        player_logs = player_logs.rename({"fg3_m": "fg3m"})
    # Step 1: per-team-game possession frame
    team_poss = team_logs.with_columns(
        (pl.col("fga") - pl.col("oreb") + pl.col("tov") + 0.44 * pl.col("fta")).alias("team_poss"),
        pl.col("min").alias("team_min"),
    ).select("game_id", "team_id", "team_poss", "team_min")
    # Step 2: join each player-game to its team-game; compute per-game player possessions
    # A team's ``min`` is the sum over its five on-court slots (240 for regulation,
    # 265/290 with overtimes), NOT the game's elapsed minutes. A player's share of
    # the team's possessions is therefore min / (team_min / 5) -- dividing by
    # team_min directly understates it fivefold, which silently inflates every
    # per-100 rate by 5x. That is invisible to SPM (its coefficients are fitted on
    # these features, so a uniform scale is absorbed) but lands straight in BPM,
    # which applies fixed published coefficients.
    player_with_poss = player_logs.join(team_poss, on=["game_id", "team_id"], how="left").with_columns(
        pl.when(pl.col("team_min") > 0)
        .then(pl.col("team_poss") * (pl.col("min") / (pl.col("team_min") / ON_COURT_PLAYERS)))
        .otherwise(0.0)
        .alias("player_poss")
    )
    # Step 3: aggregate per player
    agg = player_with_poss.group_by("player_id").agg(
        pl.col("min").sum().alias("min"),
        pl.len().alias("gp"),
        pl.col("player_poss").sum().alias("player_poss"),
        *[pl.col(s).sum().alias(s) for s in _STATS],
    )
    # Step 4: per-100 rates
    per100 = [
        pl.when(pl.col("player_poss") > 0).then(pl.col(s) / pl.col("player_poss") * 100.0).otherwise(0.0).alias(s)
        for s in _STATS
    ]
    return agg.with_columns(per100).select(
        pl.col("player_id").cast(pl.Int64),
        *_STATS,
        pl.col("min").cast(pl.Float64),
        pl.col("gp").cast(pl.Int64),
    )


def nba_box_logs(
    season: str,
    *,
    league_id: str = "00",
    season_type: str = "Regular Season",
    fetch: Optional[Callable[..., pl.DataFrame]] = None,
) -> Dict[str, pl.DataFrame]:
    """Fetch per-player and per-team game logs for a season (bulk, one call each).

    Args:
        season: NBA season in ``"2023-24"`` form.
        league_id: LeagueID (``"00"`` NBA).
        season_type: SeasonType (``"Regular Season"``).
        fetch: Injectable ``nba_stats_leaguegamelog`` replacement for offline tests.

    Returns:
        ``{"player": <per-player-game logs>, "team": <per-team-game logs>}``
        as snake-cased polars frames.

    Example:
        Fetch a season's logs (residential IP)::

            from sportsdataverse.nba.nba_box_logs import nba_box_logs
            logs = nba_box_logs("2023-24")
            print(logs["player"].shape)
    """
    get = fetch or nba_stats_leaguegamelog
    player = get(
        player_or_team_abbreviation="P",
        season=season,
        league_id=league_id,
        season_type_all_star=season_type,
    )
    team = get(
        player_or_team_abbreviation="T",
        season=season,
        league_id=league_id,
        season_type_all_star=season_type,
    )
    return {"player": player, "team": team}


#: Identity columns :func:`nba_player_identity` emits, in output order.
PLAYER_IDENTITY_SCHEMA: Dict[str, pl.DataType] = {
    "player_id": pl.Int64,
    "player_name": pl.Utf8,
    "team_id": pl.Int64,
    "team_abbreviation": pl.Utf8,
    "team_name": pl.Utf8,
    "teams": pl.Utf8,
}


def nba_player_identity(player_logs: pl.DataFrame) -> pl.DataFrame:
    """Human-readable identity for every player in a season's box logs.

    Model outputs key on ``player_id`` alone, which makes them unusable without a
    second lookup -- a leaderboard reads ``1628983`` instead of
    ``Shai Gilgeous-Alexander``. This derives the display columns from the season's
    own game logs, so they are **season-accurate**: a player's team is what he
    actually played for that year, not his current one (which is what a player
    directory would give and would silently mislabel every historical season).

    A traded player has rows for several teams. ``team_*`` is his **primary** team
    by minutes -- the one a reader means when they say "his team that season" --
    and ``teams`` lists every abbreviation he appeared for, in descending minutes,
    so a trade is visible rather than silently collapsed.

    Args:
        player_logs: Per-player-per-game rows from ``leaguegamelog`` (the
            ``player_or_team="P"`` variant), carrying ``player_id``,
            ``player_name``, ``team_id``, ``team_abbreviation``, ``team_name``
            and ``min``.

    Returns:
        One row per ``player_id`` with :data:`PLAYER_IDENTITY_SCHEMA`. An empty
        input -- or one missing any required column, ``min`` included -- gives the
        zero-row frame with that schema, so callers can join unconditionally.
        ``min`` is required rather than optional: without it every team totals
        zero minutes and "primary team" quietly degrades to whichever ``team_id``
        sorts first, which looks like an answer but is not one.

    Raises:
        None: a malformed frame yields the typed zero-row frame instead of
        raising, so a caller can join unconditionally.

    Example:
        Attach names to a model output::

            import polars as pl
            from sportsdataverse.nba import nba_player_identity

            logs = pl.DataFrame({
                "player_id": [1628983],
                "player_name": ["Shai Gilgeous-Alexander"],
                "team_id": [1610612760],
                "team_abbreviation": ["OKC"],
                "team_name": ["Oklahoma City Thunder"],
                "min": [34.0],
            })
            ratings = pl.DataFrame({"player_id": [1628983], "war": [21.9]})
            named = ratings.join(nba_player_identity(logs), on="player_id", how="left")
            print(named.select("player_name", "team_name", "war"))

        See Also:
            * `nba_api`_ -- reference Python client for stats.nba.com
            * `hoopR`_ -- R companion package for NBA/MBB data

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    # ``min`` is required, not optional: without it every team totals zero minutes
    # and the "primary" team degrades to whichever team_id sorts first -- a wrong
    # answer wearing the shape of a right one. Refuse rather than guess.
    need = {"player_id", "player_name", "team_id", "team_abbreviation", "team_name", "min"}
    if player_logs.is_empty() or not need.issubset(set(player_logs.columns)):
        return pl.DataFrame(schema=PLAYER_IDENTITY_SCHEMA)

    per_team = (
        player_logs.with_columns(pl.col("min").fill_null(0.0).cast(pl.Float64).alias("_min"))
        .group_by(["player_id", "team_id", "team_abbreviation", "team_name"])
        .agg(pl.col("_min").sum().alias("_team_min"), pl.col("player_name").last().alias("player_name"))
    )
    # Order the teams INSIDE each aggregation rather than pre-sorting and trusting
    # group_by to preserve that order -- polars only guarantees preservation with
    # maintain_order, so a pre-sort would leave BOTH the primary pick and the teams
    # string resting on undocumented behaviour. Ties break on team_id so a rebuild
    # stays byte-identical.
    _by, _desc = ["_team_min", "team_id"], [True, False]
    _first = lambda c: pl.col(c).sort_by(_by, descending=_desc).first().alias(c)  # noqa: E731
    return (
        per_team.group_by("player_id")
        .agg(
            _first("player_name"),
            _first("team_id"),
            _first("team_abbreviation"),
            _first("team_name"),
            pl.col("team_abbreviation").sort_by(_by, descending=_desc).str.join(",").alias("teams"),
        )
        .select(
            pl.col("player_id").cast(pl.Int64),
            pl.col("player_name").cast(pl.Utf8),
            pl.col("team_id").cast(pl.Int64),
            pl.col("team_abbreviation").cast(pl.Utf8),
            pl.col("team_name").cast(pl.Utf8),
            pl.col("teams").cast(pl.Utf8),
        )
        .sort("player_id")
    )
