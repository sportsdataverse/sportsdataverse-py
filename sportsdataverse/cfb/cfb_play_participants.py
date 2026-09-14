"""ESPN college-football play-participants scraper.

Single ESPN endpoint:
    sports.core.api.espn.com/v2/sports/football/leagues/college-football/events/{game_id}/competitions/{game_id}/plays?limit=1000

ESPN's per-play ``participants[]`` array is the authoritative source for which
athletes were involved in each play (passer, rusher, receiver, tackler, etc.).
This wrapper pulls the full play-list for a game, extracts the participants,
resolves each ``$ref`` URL into an ``athlete_id`` / ``position_id``, attaches
the per-athlete display name from a sibling roster lookup, and pivots the
result so each play has one row keyed by ``play_id`` with the participant
display name and id materialized as ``{type}_player_name`` /
``{type}_player_id`` columns (e.g. ``passer_player_name``).

Designed to replace the regex-based player-name extraction the
``cfb_pbp.CFBPlayProcess.__add_player_cols`` method previously did against
the freeform ``text`` column. Coverage was probed back to season 2014 (the
earliest season with reliable ESPN CFB PBP coverage) and is solid for every
sampled era — see the project diff doc for the probe table.

Caveats:

* ``$ref`` URLs are parsed for the athlete/position id (the trailing numeric
  segment). The full ``$ref`` URL is also retained so the optional
  ``resolve_missing`` pass can fetch any athlete the sidecar omitted.
* Display names come primarily from the ``cdn.espn.com/.../playbyplay``
  sidecar (the same one the legacy class uses). The sidecar is one round
  trip for the whole roster, but it is built from the box-score side and
  occasionally omits athletes who appear only in the participants payload
  (split sacks where the second sacker isn't on the leaders list, returners
  on lateral plays, etc.). When ``resolve_missing=True`` (the default),
  athletes still missing a name after the sidecar pass are fetched
  one-by-one from their canonical ``$ref`` URL and the names backfilled
  before the pivot. The fan-out is capped per game (default 50) so a
  pathological game can't run away.
* Pagination: the endpoint historically caps at one page of 1000 plays per
  game. We follow the ``pageCount`` cursor defensively in case ESPN ever
  changes that.
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.football.play_participants import espn_play_participants as _espn_play_participants


@overload
def espn_cfb_play_participants(
    game_id: int,
    *,
    raw: Literal[True],
    return_as_pandas: bool = ...,
    resolve_missing: bool = ...,
    resolve_missing_max: int = ...,
    **kwargs: Any,
) -> dict[str, Any]: ...
@overload
def espn_cfb_play_participants(
    game_id: int,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[True],
    resolve_missing: bool = ...,
    resolve_missing_max: int = ...,
    **kwargs: Any,
) -> pd.DataFrame: ...
@overload
def espn_cfb_play_participants(
    game_id: int,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    resolve_missing: bool = ...,
    resolve_missing_max: int = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def espn_cfb_play_participants(
    game_id: int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    resolve_missing: bool = True,
    resolve_missing_max: int = 50,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull ESPN per-play participants for a college-football game.

    The college-football entry point of the shared
    :func:`sportsdataverse.football.play_participants.espn_play_participants`;
    see it for the column contract (``{type}_player_name`` / ``{type}_player_id``
    scalars plus the ``{type}_player_names`` / ``{type}_player_ids`` lists per
    participant type ESPN ships).

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

            from sportsdataverse.cfb import espn_cfb_play_participants
            participants = espn_cfb_play_participants(game_id=401628334)
            print(participants.shape)
    """
    return _espn_play_participants(
        game_id,
        league="college-football",
        raw=raw,
        return_as_pandas=return_as_pandas,
        resolve_missing=resolve_missing,
        resolve_missing_max=resolve_missing_max,
        **kwargs,
    )
