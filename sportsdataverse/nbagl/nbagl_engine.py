"""G-League possession-engine shims — thin fetchers over the league-agnostic nba/ cores (league_id=20).

The NBA G-League uses the same NBA Stats API family (stats.nba.com) and the
same JSON envelope shape as the NBA.  All heavy lifting — PBP normalisation,
rotation-based on-court inference, possession segmentation, RAPM ridge
regression — is performed by the shared ``sportsdataverse.nba`` cores with
``league_id="20"`` forwarded where applicable (only
``nba_stats_gamerotation`` needs it; ``playbyplayv3`` and
``boxscoretraditionalv3`` have no ``league_id`` parameter).

Public surface
--------------
- :func:`nbagl_enhanced_pbp`    — normalised play-by-play frame
- :func:`nbagl_on_court`        — 10-player rotation-keyed frame (home+away ×5)
- :func:`nbagl_possessions`     — possession-level stint matrix (off+def ×5)
- :func:`nbagl_rapm_from_games` — per-player RAPM over a list of games
"""

from __future__ import annotations

from typing import Sequence, Union

import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away,
    parse_rotation_resultsets,
    players_on_court_from_rotation,
)
from sportsdataverse.nba.nba_possessions import attach_possession_lineups, build_possessions
from sportsdataverse.nba.nba_rapm import nba_rapm

_GLEAGUE_LEAGUE_ID = "20"

# ---------------------------------------------------------------------------
# Module-level fetch helpers — monkeypatched in offline tests
# ---------------------------------------------------------------------------


def _fetch_pbp(game_id: str) -> dict:
    from sportsdataverse.nba.nba_stats import nba_stats_playbyplayv3

    return nba_stats_playbyplayv3(game_id=game_id, return_parsed=False)


def _fetch_rotation(game_id: str) -> dict:
    from sportsdataverse.nba.nba_stats import nba_stats_gamerotation

    return nba_stats_gamerotation(
        game_id=game_id,
        league_id=_GLEAGUE_LEAGUE_ID,
        return_parsed=False,
    )


def _fetch_box(game_id: str) -> dict:
    from sportsdataverse.nba.nba_stats import nba_stats_boxscoretraditionalv3

    return nba_stats_boxscoretraditionalv3(game_id=game_id, return_parsed=False)


# ---------------------------------------------------------------------------
# Internal helper — not part of the public API
# ---------------------------------------------------------------------------


def _enhanced(game_id: str) -> pl.DataFrame:
    """Return the normalised enhanced PBP frame for *game_id* (G-League)."""
    return enhanced_pbp_from_payload(_fetch_pbp(game_id), league_id=_GLEAGUE_LEAGUE_ID)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def nbagl_enhanced_pbp(
    game_id: str,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Return a normalised enhanced play-by-play frame for a G-League game.

    Fetches the raw ``playbyplayv3`` payload from ``stats.nba.com`` via
    :func:`~sportsdataverse.nba.nba_stats.nba_stats_playbyplayv3` then
    delegates all transformation to the league-agnostic
    :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`
    core with ``league_id="20"``.  Never raises on malformed or empty
    payloads — returns a zero-row frame instead.

    Args:
        game_id: G-League game identifier string (e.g. ``"2022400003"``).
        return_as_pandas: If ``True``, convert the result to a
            :class:`pandas.DataFrame` before returning.

    Returns:
        Polars (or pandas) DataFrame with schema
        ``sportsdataverse.nba.nba_enhanced_pbp.ENHANCED_PBP_SCHEMA``.
        Key columns include ``game_id`` (Utf8), ``action_number`` (Int64),
        ``period`` (Int64), ``seconds_remaining`` (Float64),
        ``team_id`` (Int64), ``person_id`` (Int64), ``is_substitution``
        (Boolean), and one Boolean flag per event type.

    Example:
        Quick start::

            from sportsdataverse.nbagl.nbagl_engine import nbagl_enhanced_pbp
            df = nbagl_enhanced_pbp("2022400003")
            print(df.shape)

        Pandas output::

            df_pd = nbagl_enhanced_pbp("2022400003", return_as_pandas=True)
            print(type(df_pd))

        Filter substitution events::

            subs = df.filter(df["is_substitution"] == True)  # noqa: E712
            print(subs.select(["period", "seconds_remaining", "person_id"]))

        See Also:
            * `hoopR`_ — NBA G-League play-by-play in R
            * `wnba_enhanced_pbp`_ — WNBA sibling function
            * `nba_enhanced_pbp`_ — NBA sibling function

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wnba_enhanced_pbp: sportsdataverse.wnba.wnba_engine.wnba_enhanced_pbp
        .. _nba_enhanced_pbp: sportsdataverse.nba.nba_enhanced_pbp.nba_enhanced_pbp
    """
    df = _enhanced(game_id)
    return df.to_pandas() if return_as_pandas else df


def nbagl_on_court(
    game_id: str,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Return the rotation-keyed on-court player frame for a G-League game.

    Makes three network calls (play-by-play v3, game rotation,
    box-score traditional v3), infers on-court rosters from the rotation
    stints via
    :func:`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
    and returns one row per PBP action with ten Int64 player-ID columns
    (``home_player_1..5`` / ``away_player_1..5``).  All transformation is
    performed by the shared ``nba/`` core with ``league_id="20"`` forwarded
    to the rotation endpoint.  Never raises on malformed payloads.

    Args:
        game_id: G-League game identifier string (e.g. ``"2022400003"``).
        return_as_pandas: If ``True``, convert the result to a
            :class:`pandas.DataFrame` before returning.

    Returns:
        Polars (or pandas) DataFrame with one row per PBP action and
        columns ``home_player_1`` … ``home_player_5``,
        ``away_player_1`` … ``away_player_5`` (all Int64), plus the
        ``action_number`` join key.

    Example:
        Quick start::

            from sportsdataverse.nbagl.nbagl_engine import nbagl_on_court
            oc = nbagl_on_court("2022400003")
            print(oc.select(["action_number", "home_player_1"]).head())

        Pandas output::

            oc_pd = nbagl_on_court("2022400003", return_as_pandas=True)
            print(type(oc_pd))

        Join on enhanced PBP::

            from sportsdataverse.nbagl.nbagl_engine import nbagl_enhanced_pbp
            enh = nbagl_enhanced_pbp("2022400003")
            joined = enh.join(oc, on="action_number", how="left")

        See Also:
            * `hoopR`_ — NBA G-League data in R
            * `wnba_on_court`_ — WNBA sibling function
            * `nba_on_court`_ — NBA sibling function

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wnba_on_court: sportsdataverse.wnba.wnba_engine.wnba_on_court
        .. _nba_on_court: sportsdataverse.nba.nba_lineups.nba_on_court
    """
    enh = _enhanced(game_id)
    home, away = boxscore_home_away(_fetch_box(game_id))
    oc = players_on_court_from_rotation(
        enh,
        parse_rotation_resultsets(_fetch_rotation(game_id)),
        home_team_id=home,
        away_team_id=away,
    )
    return oc.to_pandas() if return_as_pandas else oc


def nbagl_possessions(
    game_id: str,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Return the possession-level lineup stint matrix for a G-League game.

    Builds possessions from the enhanced PBP via
    :func:`~sportsdataverse.nba.nba_possessions.build_possessions`, resolves
    on-court rosters via
    :func:`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
    then attaches the 5v5 lineups via
    :func:`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`.
    All transformation is performed by the shared ``nba/`` cores — no
    G-League-specific logic.  Never raises on malformed payloads.

    Args:
        game_id: G-League game identifier string (e.g. ``"2022400003"``).
        return_as_pandas: If ``True``, convert the result to a
            :class:`pandas.DataFrame` before returning.

    Returns:
        Polars (or pandas) DataFrame with schema combining
        ``POSSESSIONS_SCHEMA`` and ten lineup columns:
        ``off_player_1`` … ``off_player_5``,
        ``def_player_1`` … ``def_player_5`` (all Int64).  One row per
        possession.  Empty or malformed inputs return a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nbagl.nbagl_engine import nbagl_possessions
            poss = nbagl_possessions("2022400003")
            print(poss.shape)

        Pandas output::

            poss_pd = nbagl_possessions("2022400003", return_as_pandas=True)
            print(type(poss_pd))

        Total points check::

            total = int(poss["points"].sum())
            print(f"Total points scored: {total}")

        See Also:
            * `hoopR`_ — NBA G-League data in R
            * `wnba_possessions`_ — WNBA sibling function
            * `nba_possessions`_ — NBA sibling function

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wnba_possessions: sportsdataverse.wnba.wnba_engine.wnba_possessions
        .. _nba_possessions: sportsdataverse.nba.nba_possessions.nba_possessions
    """
    enh = _enhanced(game_id)
    home, away = boxscore_home_away(_fetch_box(game_id))
    oc = players_on_court_from_rotation(
        enh,
        parse_rotation_resultsets(_fetch_rotation(game_id)),
        home_team_id=home,
        away_team_id=away,
    )
    poss = attach_possession_lineups(build_possessions(enh), oc, enh, home_team_id=home)
    return poss.to_pandas() if return_as_pandas else poss


def _fetch_possessions(game_id: str) -> pl.DataFrame:
    """Internal helper — fetch and build possessions (polars) for *game_id*."""
    return nbagl_possessions(game_id)


def nbagl_rapm_from_games(
    game_ids: Sequence[str],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Compute per-player RAPM estimates over a sequence of G-League games.

    Iterates *game_ids*, builds the possession-level stint matrix for each
    via :func:`nbagl_possessions`, concatenates the results, and fits a
    ridge-regression RAPM model via
    :func:`~sportsdataverse.nba.nba_rapm.nba_rapm`.  Games whose possession
    frame is empty (e.g. a malformed payload) are silently skipped.  Returns
    a zero-row frame when no valid possessions are found.

    Args:
        game_ids: Sequence of G-League game identifier strings.
        return_as_pandas: If ``True``, convert the result to a
            :class:`pandas.DataFrame` before returning.

    Returns:
        Polars (or pandas) DataFrame with one row per player and columns
        ``player_id`` (Int64), ``o_rapm`` (Float64), ``d_rapm`` (Float64),
        ``rapm`` (Float64), ``off_poss`` (Int64), ``def_poss`` (Int64).

    Example:
        Quick start::

            from sportsdataverse.nbagl.nbagl_engine import nbagl_rapm_from_games
            rapm = nbagl_rapm_from_games(["2022400003", "2022400009"])
            print(rapm.sort("rapm", descending=True).head())

        Pandas output::

            rapm_pd = nbagl_rapm_from_games(["2022400003"], return_as_pandas=True)
            print(type(rapm_pd))

        Multi-season aggregation::

            import polars as pl
            game_ids = pl.read_parquet("nbagl_schedule.parquet")["game_id"].to_list()
            rapm = nbagl_rapm_from_games(game_ids)
            print(rapm.sort("rapm", descending=True).head(10))

        See Also:
            * `hoopR`_ — NBA G-League data in R
            * `wnba_rapm_from_games`_ — WNBA sibling function
            * `nba_rapm`_ — NBA sibling function

        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wnba_rapm_from_games: sportsdataverse.wnba.wnba_engine.wnba_rapm_from_games
        .. _nba_rapm: sportsdataverse.nba.nba_rapm.nba_rapm
    """
    frames: list[pl.DataFrame] = []
    for gid in game_ids:
        p = _fetch_possessions(gid)
        if not p.is_empty():
            frames.append(p)
    if not frames:
        out = nba_rapm(pl.DataFrame())
        return out.to_pandas() if return_as_pandas else out
    out = nba_rapm(pl.concat(frames, how="diagonal_relaxed"))
    return out.to_pandas() if return_as_pandas else out
