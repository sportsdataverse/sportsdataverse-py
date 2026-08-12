"""WNBA possession-engine shims — thin fetchers over the league-agnostic nba/ cores (league_id=10).

The WNBA uses the same NBA Stats API family (stats.wnba.com) and the same
JSON envelope shape as the NBA.  All heavy lifting — PBP normalisation,
rotation-based on-court inference, possession segmentation, RAPM ridge
regression — is performed by the shared ``sportsdataverse.nba`` cores with
``league_id="10"`` forwarded where applicable (only
``wnba_stats_gamerotation`` needs it; ``playbyplayv3`` and
``boxscoretraditionalv3`` have no ``league_id`` parameter).

Public surface
--------------
- :func:`wnba_enhanced_pbp`   — normalised play-by-play frame
- :func:`wnba_on_court`       — 10-player rotation-keyed frame (home+away ×5)
- :func:`wnba_possessions`    — possession-level stint matrix (off+def ×5)
- :func:`wnba_play_context`   — possessions + the CTG play-context surface
- :func:`wnba_rapm_from_games` — per-player RAPM over a list of games
"""

from __future__ import annotations

import os
from typing import Sequence, Union

import pandas as pd
import polars as pl

from sportsdataverse.nba import nba_play_context_constants as C
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_play_context import add_play_context
from sportsdataverse.nba.nba_lineups import (
    boxscore_home_away,
    parse_rotation_resultsets,
    players_on_court_from_rotation,
)
from sportsdataverse.nba.nba_possessions import attach_possession_lineups, build_possessions
from sportsdataverse.nba.nba_rapm import nba_rapm

_WNBA_LEAGUE_ID = "10"

# ---------------------------------------------------------------------------
# Raw JSON store (read-through, WNBA env namespace)
# ---------------------------------------------------------------------------
# Reuses the league-agnostic store in nba_possessions (its season decode is
# already WNBA-aware — game ids prefixed "10" are single calendar years) but
# with WNBA env vars, so a WNBA compile can read a wehoop-wnba-stats-raw
# checkout offline via SDV_PY_WNBA_RAW_JSON_DIR as a pure consumer
# (SDV_PY_WNBA_RAW_JSON_READONLY=1) — the -raw sweep stays the only writer.
# Read-only is OFFLINE: an uncaptured game raises RawStoreMissError from every
# function in this module rather than reaching stats.wnba.com, so a "read the
# committed store" compile can't silently complete itself over the network.
# Resolving the WNBA env here (not via the store's own NBA-named fallback)
# keeps the namespaces separate: unset -> store off, never an NBA-env bleed.


def _wnba_store_dir() -> str:
    """WNBA raw-store root, or ``""`` (store disabled) when the env var is unset."""
    return os.environ.get("SDV_PY_WNBA_RAW_JSON_DIR") or ""


def _wnba_readonly() -> bool:
    """Whether the WNBA store is read-only, i.e. fully offline (consumer mode).

    Read-only means the committed store is the only source: an uncaptured game
    raises :class:`~sportsdataverse.errors.RawStoreMissError` instead of hitting
    stats.wnba.com. Default is write-on-miss (fetch + persist).
    """
    return bool(os.environ.get("SDV_PY_WNBA_RAW_JSON_READONLY"))


# ---------------------------------------------------------------------------
# Module-level fetch helpers — monkeypatched in offline tests
# ---------------------------------------------------------------------------


def _fetch_pbp(game_id: str) -> dict:
    from sportsdataverse.nba.nba_possessions import _through_raw_store
    from sportsdataverse.wnba.wnba_stats import wnba_stats_playbyplayv3

    return _through_raw_store(
        "playbyplayv3",
        game_id,
        lambda: wnba_stats_playbyplayv3(game_id=game_id, return_parsed=False),
        store_dir=_wnba_store_dir(),
        readonly=_wnba_readonly(),
    )


def _fetch_rotation(game_id: str) -> dict:
    from sportsdataverse.nba.nba_possessions import _through_raw_store
    from sportsdataverse.wnba.wnba_stats import wnba_stats_gamerotation

    return _through_raw_store(
        "gamerotation",
        game_id,
        lambda: wnba_stats_gamerotation(
            game_id=game_id,
            league_id=_WNBA_LEAGUE_ID,
            return_parsed=False,
        ),
        store_dir=_wnba_store_dir(),
        readonly=_wnba_readonly(),
    )


def _fetch_box(game_id: str) -> dict:
    from sportsdataverse.nba.nba_possessions import _through_raw_store
    from sportsdataverse.wnba.wnba_stats import wnba_stats_boxscoretraditionalv3

    return _through_raw_store(
        "boxscoretraditionalv3",
        game_id,
        lambda: wnba_stats_boxscoretraditionalv3(game_id=game_id, return_parsed=False),
        store_dir=_wnba_store_dir(),
        readonly=_wnba_readonly(),
    )


# ---------------------------------------------------------------------------
# Internal helper — not part of the public API
# ---------------------------------------------------------------------------


def _enhanced(game_id: str) -> pl.DataFrame:
    """Return the normalised enhanced PBP frame for *game_id* (WNBA)."""
    return enhanced_pbp_from_payload(_fetch_pbp(game_id), league_id=_WNBA_LEAGUE_ID)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def wnba_enhanced_pbp(
    game_id: str,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Return a normalised enhanced play-by-play frame for a WNBA game.

    Fetches the raw ``playbyplayv3`` payload from ``stats.wnba.com`` via
    :func:`~sportsdataverse.wnba.wnba_stats.wnba_stats_playbyplayv3` then
    delegates all transformation to the league-agnostic
    :func:`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`
    core with ``league_id="10"``.  Never raises on malformed or empty
    payloads — returns a zero-row frame instead.

    Args:
        game_id: WNBA game identifier string (e.g. ``"1022400001"``).
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

            from sportsdataverse.wnba.wnba_engine import wnba_enhanced_pbp
            df = wnba_enhanced_pbp("1022400001")
            print(df.shape)

        Pandas output::

            df_pd = wnba_enhanced_pbp("1022400001", return_as_pandas=True)
            print(type(df_pd))

        Filter substitution events::

            subs = df.filter(df["is_substitution"] == True)  # noqa: E712
            print(subs.select(["period", "seconds_remaining", "person_id"]))

        See Also:
            * `wehoop`_ — WNBA/WBB play-by-play in R
            * `nba_enhanced_pbp`_ — NBA sibling function

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_enhanced_pbp: sportsdataverse.nba.nba_enhanced_pbp.nba_enhanced_pbp
    """
    df = _enhanced(game_id)
    return df.to_pandas() if return_as_pandas else df


def wnba_on_court(
    game_id: str,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Return the rotation-keyed on-court player frame for a WNBA game.

    Makes three network calls (play-by-play v3, game rotation,
    box-score traditional v3), infers on-court rosters from the rotation
    stints via
    :func:`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
    and returns one row per PBP action with ten Int64 player-ID columns
    (``home_player_1..5`` / ``away_player_1..5``).  All transformation is
    performed by the shared ``nba/`` core with ``league_id="10"`` forwarded
    to the rotation endpoint.  Never raises on malformed payloads.

    Args:
        game_id: WNBA game identifier string (e.g. ``"1022400001"``).
        return_as_pandas: If ``True``, convert the result to a
            :class:`pandas.DataFrame` before returning.

    Returns:
        Polars (or pandas) DataFrame with one row per PBP action and
        columns ``home_player_1`` … ``home_player_5``,
        ``away_player_1`` … ``away_player_5`` (all Int64), plus the
        ``action_number`` join key.

    Example:
        Quick start::

            from sportsdataverse.wnba.wnba_engine import wnba_on_court
            oc = wnba_on_court("1022400001")
            print(oc.select(["action_number", "home_player_1"]).head())

        Pandas output::

            oc_pd = wnba_on_court("1022400001", return_as_pandas=True)
            print(type(oc_pd))

        Join on enhanced PBP::

            from sportsdataverse.wnba.wnba_engine import wnba_enhanced_pbp
            enh = wnba_enhanced_pbp("1022400001")
            joined = enh.join(oc, on="action_number", how="left")

        See Also:
            * `wehoop`_ — WNBA/WBB data in R
            * `nba_on_court`_ — NBA sibling function

        .. _wehoop: https://wehoop.sportsdataverse.org
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


def wnba_possessions(
    game_id: str,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Return the possession-level lineup stint matrix for a WNBA game.

    Builds possessions from the enhanced PBP via
    :func:`~sportsdataverse.nba.nba_possessions.build_possessions`, resolves
    on-court rosters via
    :func:`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
    then attaches the 5v5 lineups via
    :func:`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`.
    All transformation is performed by the shared ``nba/`` cores — no WNBA-
    specific logic.  Never raises on malformed payloads.

    Args:
        game_id: WNBA game identifier string (e.g. ``"1022400001"``).
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

            from sportsdataverse.wnba.wnba_engine import wnba_possessions
            poss = wnba_possessions("1022400001")
            print(poss.shape)

        Pandas output::

            poss_pd = wnba_possessions("1022400001", return_as_pandas=True)
            print(type(poss_pd))

        Total points check::

            total = int(poss["points"].sum())
            print(f"Total points scored: {total}")

        See Also:
            * `wehoop`_ — WNBA/WBB data in R
            * `nba_possessions`_ — NBA sibling function

        .. _wehoop: https://wehoop.sportsdataverse.org
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


def wnba_play_context(
    game_id: str,
    *,
    transition_seconds: float = C.DEFAULT_TRANSITION_SECONDS,
    transition_variant: str = C.DEFAULT_TRANSITION_VARIANT,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Return a WNBA game's possessions with the full CTG play-context surface.

    WNBA sibling of
    :func:`~sportsdataverse.nba.nba_play_context.nba_play_context` — the
    Cleaning the Glass recreation (possession start-type taxonomy + the
    halfcourt / transition / putback contexts + CTG's garbage-time and heave
    filters). One network call (``playbyplayv3`` on ``stats.wnba.com``); every
    transformation is done by the league-agnostic
    :func:`~sportsdataverse.nba.nba_play_context.add_play_context` core, so
    there is **no WNBA-specific classification logic** to drift.

    Two caveats worth stating plainly:

    * **CTG is NBA-only.** There is no published WNBA play-context table to
      calibrate against, so ``transition_seconds`` inherits the NBA's fitted
      6.0 s default. The WNBA fixtures land inside the NBA's transition-frequency
      gate at that value (``tests/wnba/test_wnba_play_context_shim.py``), which
      is a sanity check on the shared engine — not evidence that 6.0 s is the
      *right* WNBA cutoff. Re-fit it if a WNBA oracle ever appears.
    * Shot-zone boundaries are league-agnostic (feet from the rim), and the
      corner-three test uses the same legacy coordinates, which the WNBA feed
      also ships.

    Args:
        game_id: WNBA game identifier (e.g. ``"1022400001"``).
        transition_seconds: Transition initial-play cutoff, in seconds.
        transition_variant: See
            :func:`~sportsdataverse.nba.nba_play_context.add_transition`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The possession frame (``POSSESSIONS_SCHEMA``) plus
        :data:`~sportsdataverse.nba.nba_play_context.PLAY_CONTEXT_POSSESSIONS_SCHEMA`.
        Empty or malformed payloads return a zero-row frame — never raises on payload
        content.

    Raises:
        ValueError: when *transition_variant* is not one of
            :data:`~sportsdataverse.nba.nba_play_context_constants.TRANSITION_VARIANTS`
            (propagated from
            :func:`~sportsdataverse.nba.nba_play_context.add_transition`).

    Example:
        Quick start::

            from sportsdataverse.wnba.wnba_engine import wnba_play_context
            poss = wnba_play_context("1022400001")
            print(poss["possession_start_type_ctg"].value_counts())

        Transition rate (CTG's default filtered view)::

            import polars as pl
            clean = poss.filter(
                (pl.col("is_garbage_time") == False)  # noqa: E712
                & (pl.col("is_heave_possession") == False)  # noqa: E712
            )
            print(clean["is_transition"].mean())

        See Also:
            * `wehoop`_ — WNBA/WBB data in R
            * `nba_play_context`_ — NBA sibling function

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_play_context: sportsdataverse.nba.nba_play_context.nba_play_context
    """
    out = add_play_context(
        _enhanced(game_id),
        transition_seconds=transition_seconds,
        transition_variant=transition_variant,
    )
    return out.to_pandas() if return_as_pandas else out


def _fetch_possessions(game_id: str) -> pl.DataFrame:
    """Internal helper — fetch and build possessions (polars) for *game_id*."""
    return wnba_possessions(game_id)


def wnba_rapm_from_games(
    game_ids: Sequence[str],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Compute per-player RAPM estimates over a sequence of WNBA games.

    Iterates *game_ids*, builds the possession-level stint matrix for each
    via :func:`wnba_possessions`, concatenates the results, and fits a
    ridge-regression RAPM model via
    :func:`~sportsdataverse.nba.nba_rapm.nba_rapm`.  Games whose possession
    frame is empty (e.g. a malformed payload) are silently skipped.  Returns
    a zero-row frame when no valid possessions are found.

    Args:
        game_ids: Sequence of WNBA game identifier strings.
        return_as_pandas: If ``True``, convert the result to a
            :class:`pandas.DataFrame` before returning.

    Returns:
        Polars (or pandas) DataFrame with one row per player and columns
        ``player_id`` (Int64), ``o_rapm`` (Float64), ``d_rapm`` (Float64),
        ``rapm`` (Float64), ``off_poss`` (Int64), ``def_poss`` (Int64).

    Example:
        Quick start::

            from sportsdataverse.wnba.wnba_engine import wnba_rapm_from_games
            rapm = wnba_rapm_from_games(["1022400001", "1022400003"])
            print(rapm.sort("rapm", descending=True).head())

        Pandas output::

            rapm_pd = wnba_rapm_from_games(["1022400001"], return_as_pandas=True)
            print(type(rapm_pd))

        Multi-season aggregation::

            import polars as pl
            game_ids = pl.read_parquet("wnba_schedule.parquet")["game_id"].to_list()
            rapm = wnba_rapm_from_games(game_ids)
            print(rapm.sort("rapm", descending=True).head(10))

        See Also:
            * `wehoop`_ — WNBA/WBB data in R
            * `nba_rapm`_ — NBA sibling function

        .. _wehoop: https://wehoop.sportsdataverse.org
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
