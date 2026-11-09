"""NBA / WNBA WAR and VORP (BB-D26): ``nba.player_war``.

Our own wins and value above replacement, from our own ratings: BPM, RAPM (``adj_rapm``) and
BOBAN (``boban_filtered_skill``) of the ``nba_player_impact`` release, team-anchored and centred
by :mod:`sportsdataverse._common.player_war`, which documents the method, the replacement level
and the minutes basis. ``league_id="10"`` reads the WNBA releases;
:mod:`sportsdataverse.wnba.wnba_player_war` binds it.

The ``nba_stats_possessions`` release gives each player's on-court possessions per team and each
team's margin; the ``nba_stats_schedules`` release picks the regular-season games (BB-D3's
choice: NBA Cup group and knockout games count, the Cup final does not).

Two producer issues are handled here rather than read through: the published ``bpm`` has its team
constant five times too large (repaired by
:func:`~sportsdataverse._common.player_war.repair_bpm_team_constant`), and the ``o_adj_rapm`` /
``d_adj_rapm`` split of releases built before November 2026 carries a +11 / -11 offset, which
``nba_adj_rapm`` no longer fits (only the net ``adj_rapm`` is read, and it is re-anchored). The release's own ``war`` column is not used.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import polars as pl

from sportsdataverse._common.player_war import player_war, repair_bpm_team_constant, season_rows
from sportsdataverse.nba.nba_team_season_rates import _league_loaders

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["nba_player_war"]

_LEAGUE = {"00": "nba", "10": "wnba"}
#: release column -> source name (``rating_<s>`` / ``war_<s>``).
_SOURCES = {"bpm": "bpm", "adj_rapm": "rapm", "boban_filtered_skill": "boban"}
_RS = "Regular Season"


def _possession_games(possessions: pl.DataFrame, schedule: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """On-court possessions per player, team and game, and each team's game, over the regular season.

    Args:
        possessions: ``load_nba_stats_possessions`` / ``load_wnba_stats_possessions`` rows.
        schedule: The matching ``*_stats_schedules`` rows; their ``regular-season`` games are kept.

    Returns:
        ``(player_games, team_games)``: one row per ``(season, team_id, game_id, player_id)`` with
        ``poss`` (possessions on the floor, offensive plus defensive over 2); and one row per
        ``(season, team_id, game_id)`` with the team's ``poss`` (offensive plus defensive over 2)
        and ``margin`` (points for minus against). Points are ``2 * fg2m + 3 * fg3m + ftm`` and
        every possession row counts (BB-D3). :func:`~sportsdataverse._common.player_war.season_rows`
        sums them to the season.
    """
    games = schedule.filter(pl.col("season_type") == "regular-season").select(pl.col("game_id").cast(pl.Utf8))
    p = possessions.with_columns(pl.col("game_id").cast(pl.Utf8)).join(games, on="game_id", how="semi")
    pts = 2 * pl.col("fg2m") + 3 * pl.col("fg3m") + pl.col("ftm")
    sides = [("offense_team_id", "off_player"), ("defense_team_id", "def_player")]
    on_court = pl.concat(
        p.select("season", pl.col(team).alias("team_id"), "game_id", pl.col(f"{slot}_{k}").alias("player_id"))
        for team, slot in sides
        for k in range(1, 6)
    ).drop_nulls("player_id")
    player_games = on_court.group_by("season", "team_id", "game_id", "player_id").agg((pl.len() / 2).alias("poss"))
    team = [
        p.group_by("season", pl.col(t).alias("team_id"), "game_id").agg(
            pl.len().alias(f"_n{i}"), pts.sum().alias(f"_p{i}")
        )
        for i, t in enumerate(("offense_team_id", "defense_team_id"))
    ]
    team_games = (
        team[0]
        .join(team[1], on=["season", "team_id", "game_id"], how="full", coalesce=True)
        .fill_null(0)
        .select(
            "season",
            "team_id",
            "game_id",
            ((pl.col("_n0") + pl.col("_n1")) / 2).alias("poss"),
            (pl.col("_p0") - pl.col("_p1")).cast(pl.Float64).alias("margin"),
        )
    )
    keys = {"season": pl.Int64, "team_id": pl.Int64}
    return player_games.cast({**keys, "player_id": pl.Int64}), team_games.cast(keys)


def _rating_stints(stints: pl.DataFrame, net: pl.DataFrame, impact: pl.DataFrame) -> pl.DataFrame:
    """Attach the release's ratings to the possession stints, with BPM's team constant repaired.

    Args:
        stints: :func:`~sportsdataverse._common.player_war.season_rows` player rows.
        net: :func:`~sportsdataverse._common.player_war.season_rows` team rows.
        impact: ``load_nba_player_impact`` / ``load_wnba_player_impact`` rows (the
            ``Regular Season`` rows are read).

    Returns:
        ``stints`` with ``player_name``, ``min`` (his season minutes split by possessions) and the
        ``bpm`` / ``rapm`` / ``boban`` ratings (null for a player the release does not rate).
    """
    rs = impact.filter(pl.col("season_type") == _RS).select(
        pl.col("season").cast(pl.Int64),
        pl.col("team_id").cast(pl.Int64),
        pl.col("player_id").cast(pl.Int64),
        "player_name",
        "min",
        *_SOURCES,
    )
    rs = repair_bpm_team_constant(rs, net).rename(_SOURCES)
    assert stints.schema["player_id"] == rs.schema["player_id"], (
        "player_id dtype differs between possessions and impact"
    )
    return stints.join(rs.drop("team_id"), on=["season", "player_id"], how="left", validate="m:1").with_columns(
        (pl.col("min") * pl.col("poss") / pl.col("poss").sum().over("season", "player_id")).alias("min")
    )


@overload
def nba_player_war(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def nba_player_war(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: Literal[True]
) -> pd.DataFrame: ...


def nba_player_war(
    seasons: int | list[int], *, league_id: str = "00", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """WAR and VORP per player-season, from our BPM, RAPM and BOBAN (regular season).

    Each rating is anchored to the team (its players' possession-weighted mean rating over the
    season is the team's net rating / 5; it is a season constraint, not an estimate for each
    five-man lineup) and centred on the league, so 0 is a league-average player. ``vorp`` is points per
    100 team possessions above a replacement player over one team's season of possessions
    (Basketball-Reference's VORP scale); ``war`` is wins above replacement. A replacement-level
    player has 0, and a season's ``war`` adds up to the league's wins minus those of a team of
    replacement players. ``rating`` / ``war`` are the mean of the three sources; ``rating_<s>``
    / ``war_<s>`` are each one. The replacement level and points per win are fitted per league
    (:data:`~sportsdataverse._common.player_war.REPLACEMENT_LEVEL`,
    :data:`~sportsdataverse._common.player_war.PTS_PER_WIN`).

    Args:
        seasons: A season or list of seasons, named by the year it ends (``2026`` is 2025-26).
            The WNBA season is its calendar year.
        league_id: ``"00"`` NBA or ``"10"`` WNBA.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per ``(season, player_id)`` (stats.nba.com ids, Int64): ``player_name``,
        ``team_id`` (the team he played most possessions for), ``teams`` (how many he played
        for), ``min``, ``poss`` (on-court possessions, offensive plus defensive over 2),
        ``rating`` and ``rating_bpm`` / ``rating_rapm`` / ``rating_boban`` (anchored, per 100),
        ``vorp``, ``war`` with ``war_lo`` / ``war_hi`` (a nominal 90% interval from a bootstrap over each
        team-season's games: the sampling uncertainty of the team side and the playing time) and
        ``war_bpm`` / ``war_rapm`` / ``war_boban`` (the source spread), with ``_rank``
        (1 = most), ``_pct`` and ``_n`` (possessions) for ``vorp`` and ``war`` among every player
        of the season. Sorted by season and ``war``. Empty input returns that schema with zero
        rows.

    Raises:
        ValueError: If ``league_id`` is not ``"00"`` or ``"10"`` (checked before any data is loaded).

    Example:
        Quick start::

            from sportsdataverse.nba.nba_player_war import nba_player_war
            war = nba_player_war(2026)
            war.select("player_name", "poss", "rating", "vorp", "war").head(10)

        WNBA via ``league_id``::

            wnba = nba_player_war(2025, league_id="10")
    """
    if league_id not in _LEAGUE:
        raise ValueError(f"league_id must be '00' (NBA) or '10' (WNBA), got {league_id!r}")
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    load_possessions, load_schedule, offset = _league_loaders(league_id)
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import load_wnba_player_impact as load_impact
    else:
        from sportsdataverse.nba.nba_loaders import load_nba_player_impact as load_impact
    loader_seasons = [s - offset for s in seasons_list]
    player_games, team_games = _possession_games(load_possessions(loader_seasons), load_schedule(loader_seasons))
    stints, net = season_rows(player_games, team_games)
    stints = _rating_stints(stints, net, load_impact(seasons_list))
    out = player_war(
        stints,
        net,
        league=_LEAGUE[league_id],
        sources=list(_SOURCES.values()),
        team_games=team_games,
        player_games=player_games,
    )
    return out.to_pandas() if return_as_pandas else out
