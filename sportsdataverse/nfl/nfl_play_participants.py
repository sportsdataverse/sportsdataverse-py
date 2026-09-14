"""ESPN NFL play-participants scraper.

The NFL entry point of the shared
:func:`sportsdataverse.football.play_participants.espn_play_participants` --
the same ``sports.core.api.espn.com`` plays endpoint and ``cdn.espn.com``
playbyplay sidecar the college-football scraper reads, with ``nfl`` in the
league segment.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import polars as pl

from sportsdataverse.football.play_participants import espn_play_participants

__all__ = ["espn_nfl_play_participants"]


def espn_nfl_play_participants(
    game_id: int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    resolve_missing: bool = True,
    resolve_missing_max: int = 50,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull ESPN per-play participants for an NFL game.

    One row per play keyed by ``play_id`` with ``{type}_player_name`` /
    ``{type}_player_id`` scalars (first occurrence) and ``{type}_player_names``
    / ``{type}_player_ids`` lists for every participant type ESPN ships
    (``passer``, ``rusher``, ``receiver``, ``tackler``, ``sacked_by``,
    ``forced_by``, ``pass_defender``, ``kicker``, ``punter``, ``returner``,
    ``recoverer``, ``scorer``, ``pat_scorer``, ``penalized``, ``assisted_by``).
    ``NFLPlayProcess`` runs it on the live path to overwrite the text-extracted
    names and ids.

    Args:
        game_id: ESPN game / event identifier.
        raw: If True, returns the raw list of play-items dicts.
        return_as_pandas: If True, returns a pandas DataFrame; otherwise polars.
        resolve_missing: Fetch athletes the sidecar omits from their ``$ref``.
        resolve_missing_max: Cap on those per-athlete requests (default 50).
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        Polars (or pandas) DataFrame, one row per play; the raw play dicts when
        ``raw=True``.

    Example:
        Quick start::

            from sportsdataverse.nfl import espn_nfl_play_participants
            participants = espn_nfl_play_participants(game_id=401872922)
            print(participants.select("play_id", "passer_player_name", "passer_player_id").head())
    """
    return espn_play_participants(
        game_id,
        league="nfl",
        raw=raw,
        return_as_pandas=return_as_pandas,
        resolve_missing=resolve_missing,
        resolve_missing_max=resolve_missing_max,
        **kwargs,
    )
