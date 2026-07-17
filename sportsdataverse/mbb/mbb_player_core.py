"""ESPN MBB athlete core records -- identity + bio season-builder release producer.

The core-v2 ``/athletes/{id}`` resource is league-neutral: nba/wnba/mbb/wbb all
ship the same payload shape (30 keys common to all four; the deltas -- pro-only
``contract``/``seasons``, college-only ``proAthlete``/``flag`` -- are optional
fields this producer already gates on). So this re-exports the NBA
implementation rather than forking a second copy, exactly as
``mbb_game_officials`` re-exports ``helper_nba_officials``.

See :mod:`sportsdataverse.nba.nba_player_core` for the two traps this encodes:
``current_team_id`` is the athlete's CURRENT team (not their season team), and
bio is a Type-1 overwriting snapshot (not era-correct).
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nba.nba_player_core import helper_nba_player_core

__all__ = ["helper_mbb_player_core"]


def helper_mbb_player_core(payload: dict, *, athlete_id: int | str) -> pl.DataFrame:
    """Project one ESPN core-v2 athlete record into the released player_core row.

    Delegates to :func:`sportsdataverse.nba.helper_nba_player_core` -- the
    core-v2 athlete resource is identical across leagues.

    Args:
        payload: One athlete's ``mbb/player_core/json/{athlete_id}.json`` as a
            dict (ESPN core-v2 ``/athletes/{id}``).
        athlete_id: ESPN athlete id. **Required and not inferred** -- callers
            pass the id from the file path. The released dtype is Int64.

    Returns:
        pl.DataFrame: Exactly one row carrying the full documented column set
        (absent fields null); empty frame for an empty payload.
        ``current_team_id`` is the CURRENT team, not the season team.

    Example:
        Quick start::

            import json
            from sportsdataverse.mbb import helper_mbb_player_core
            payload = json.load(open("4433176.json", encoding="utf-8"))
            df = helper_mbb_player_core(payload, athlete_id=4433176)
            print(df.select("full_name", "display_height").row(0))

    See Also:
        * `hoopR`_ -- R sister package for these releases.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return helper_nba_player_core(payload, athlete_id=athlete_id)
