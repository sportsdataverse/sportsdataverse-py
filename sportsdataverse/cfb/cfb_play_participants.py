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

import logging
import re
from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, underscore

logger = logging.getLogger("sdv.cfb.cfb_play_participants")
logger.addHandler(logging.NullHandler())

# Pre-compiled regex for parsing the trailing numeric id out of an ESPN ``$ref``
# URL like ``http://sports.core.api.espn.com/v2/.../athletes/4567010?lang=en``.
_ID_FROM_REF: re.Pattern[str] = re.compile(r"/(?:athletes|positions)/(\d+)")

# Schema returned when the endpoint has no participant data (e.g. forfeited
# games or games with no PBP coverage). Keeps the downstream join schema
# stable.
_EMPTY_SCHEMA: dict[str, type[pl.DataType] | pl.DataType] = {
    "game_id": pl.Int64,
    "play_id": pl.Int64,
}


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

    Args:
        game_id: ESPN game / event identifier.
        raw: If True, returns the raw list of play-items dicts (after
            following pagination) before any flattening.
        return_as_pandas: If True, returns a pandas DataFrame; otherwise polars.
        resolve_missing: If True (default), athletes that the
            ``cdn.espn.com`` sidecar omits are fetched one-by-one from
            their canonical ESPN ``$ref`` URL so the resulting frame has
            populated ``*_player_name`` / ``*_player_names`` columns
            wherever an ``*_player_id`` is non-null. Setting this to
            False skips the extra HTTP fan-out and reproduces the
            pre-enhancement behavior — rows may then ship with
            ``*_player_id`` populated but ``*_player_name`` null on the
            handful of athletes the sidecar misses (most visible on
            split sacks, multi-lateral returns, and older games).
        resolve_missing_max: Hard cap on the number of per-athlete
            ``$ref`` requests issued for a single game. Defaults to 50,
            which comfortably covers every probed game (typical max is
            ≤8 unique missing athletes). If breached, a warning is
            logged and the remaining missing athletes are left with
            null names. Ignored when ``resolve_missing=False``.
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        Polars (or pandas) DataFrame, one row per play. Columns include
        ``game_id``, ``play_id``, and TWO column families for every
        participant ``type`` ESPN ships for the game (typical types:
        ``passer``, ``rusher``, ``receiver``, ``tackler``, ``sacked_by``,
        ``forced_by``, ``pass_defender``, ``kicker``, ``punter``,
        ``returner``, ``recoverer``, ``scorer``, ``pat_scorer``,
        ``penalized``, ``assisted_by``):

        * **Scalar** — ``{type}_player_id`` / ``{type}_player_name``: the
          first occurrence of that participant type on the play. Backwards
          compatible with the legacy regex-extractor shape.
        * **List** — ``{type}_player_ids`` / ``{type}_player_names``:
          ``List(Utf8)`` columns containing **every** occurrence of that
          participant type on the play, in the order ESPN shipped them.
          Plays with no participant of a given type carry an empty list
          ``[]`` (not null) for downstream consumption simplicity. This
          family preserves multi-entry participant types (split sacks
          where ESPN ships two ``sackedBy`` entries, multi-tacklers,
          etc.) that the scalar family collapses to first-only.

        If ``raw=True``, returns the parsed JSON list of play dicts.

    Raises:
        sportsdataverse.errors.NoESPNDataError: ESPN returned 404.
        requests.exceptions.RequestException: Other network failures after retries.

    Example:
        Quick start::

            from sportsdataverse.cfb import espn_cfb_play_participants
            participants = espn_cfb_play_participants(game_id=401628334)
            print(participants.shape)

        Skip the per-athlete fan-out for speed::

            participants_fast = espn_cfb_play_participants(
                game_id=401628334,
                resolve_missing=False,
            )

        Pipeline next step (join onto play-by-play frame)::

            from sportsdataverse.cfb import CFBPlayProcess
            pbp = CFBPlayProcess(gameId=401628334).espn_cfb_pbp()
            plays = pbp["plays"]
            joined = plays.join(participants, how="left", left_on="id", right_on="play_id")

        See Also:
            * `cfbfastR <https://cfbfastR.sportsdataverse.org>`_ -- R sister package for CFB PBP
            * `nflverse <https://nflverse.nflverse.com>`_ -- companion data ecosystem for the NFL
    """
    plays_raw = _download_plays(game_id, **kwargs)

    if raw:
        return {"items": plays_raw}

    athlete_lookup = _download_athlete_lookup(game_id, **kwargs)

    long = _build_long_frame(plays_raw, athlete_lookup, game_id)

    if long.is_empty():
        empty = pl.DataFrame(schema=_EMPTY_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    if resolve_missing:
        long = _resolve_missing_athletes(long, max_fetches=resolve_missing_max, **kwargs)

    wide = _pivot_wide(long)

    return wide.to_pandas() if return_as_pandas else wide


def _download_plays(game_id: int, **kwargs: Any) -> list[dict[str, Any]]:
    """Pull every play dict for ``game_id``, following ``pageCount``."""
    base = (
        f"https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/"
        f"events/{game_id}/competitions/{game_id}/plays"
    )
    params: dict[str, Any] = {"limit": 1000, "page": 1}
    out: list[dict[str, Any]] = []
    while True:
        resp = download(base, params=params, **kwargs)
        body = resp.json()
        items = body.get("items") or []
        if isinstance(items, list):
            out.extend(item for item in items if isinstance(item, dict))
        page_count = int(body.get("pageCount") or 0)
        page_index = int(body.get("pageIndex") or params["page"])
        if page_index >= page_count or not items:
            break
        params["page"] = page_index + 1
    return out


def _download_athlete_lookup(game_id: int, **kwargs: Any) -> dict[str, str]:
    """Build an ``athlete_id -> display_name`` map from the playbyplay sidecar.

    Uses the ``cdn.espn.com/core/college-football/playbyplay`` XHR endpoint
    that the legacy class already relies on for the box-score side; the
    payload carries every athlete that appears in the game, keyed by id.
    """
    url = f"https://cdn.espn.com/core/college-football/playbyplay?xhr=1&gameId={game_id}"
    try:
        resp = download(url, **kwargs)
        sidecar = resp.json()
    except Exception:  # noqa: BLE001
        # Sidecar is best-effort; participants table is still useful with
        # only ids if the sidecar is unavailable for some reason.
        return {}

    pkg = sidecar.get("__gamepackage__") or {}
    player_hash = pkg.get("playerHash") or {}
    lookup: dict[str, str] = {}
    if isinstance(player_hash, dict):
        for ath_id, payload in player_hash.items():
            if not isinstance(payload, dict):
                continue
            athlete = (payload.get("json") or {}).get("athlete") or {}
            display = athlete.get("displayName") or athlete.get("fullName")
            if display:
                lookup[str(ath_id)] = str(display)
    return lookup


def _id_from_ref(ref: str | None) -> str | None:
    """Extract the trailing numeric id from an ESPN ``$ref`` URL."""
    if not ref:
        return None
    match = _ID_FROM_REF.search(ref)
    return match.group(1) if match else None


def _build_long_frame(
    plays_raw: list[dict[str, Any]],
    athlete_lookup: dict[str, str],
    game_id: int,
) -> pl.DataFrame:
    """Flatten ``plays[*].participants[*]`` into one row per (play, participant).

    The athlete ``$ref`` URL is retained on each row so the downstream
    ``_resolve_missing_athletes`` pass can fetch the canonical display
    name without having to reconstruct the URL from the bare id.
    """
    rows: list[dict[str, Any]] = []
    for play in plays_raw:
        play_id = play.get("id")
        if play_id is None:
            continue
        participants = play.get("participants") or []
        if not isinstance(participants, list):
            continue
        for k in participants:
            if not isinstance(k, dict):
                continue
            athlete_ref = (k.get("athlete") or {}).get("$ref")
            position_ref = (k.get("position") or {}).get("$ref")
            athlete_id = _id_from_ref(athlete_ref)
            participant_type = k.get("type")
            if athlete_id is None or not participant_type:
                continue
            rows.append(
                {
                    "game_id": int(game_id),
                    "play_id": str(play_id),
                    "athlete_id": athlete_id,
                    "athlete_ref": athlete_ref,
                    "position_id": _id_from_ref(position_ref),
                    "participant_type": underscore(str(participant_type)),
                    "player_name": athlete_lookup.get(str(athlete_id)),
                },
            )

    if not rows:
        return pl.DataFrame(
            schema={
                "game_id": pl.Int64,
                "play_id": pl.Utf8,
                "athlete_id": pl.Utf8,
                "athlete_ref": pl.Utf8,
                "position_id": pl.Utf8,
                "participant_type": pl.Utf8,
                "player_name": pl.Utf8,
            },
        )

    return pl.DataFrame(rows)


def _resolve_missing_athletes(
    long: pl.DataFrame,
    *,
    max_fetches: int,
    **kwargs: Any,
) -> pl.DataFrame:
    """Backfill ``player_name`` nulls by fetching each missing athlete's ``$ref``.

    Walks every row in the long frame where ``athlete_id`` is non-null but
    ``player_name`` is null, dedupes by ``athlete_id``, and issues one
    ``download()`` call per unique athlete. Results are written back into
    the long frame's ``player_name`` column. Both pivots downstream then
    inherit the filled names automatically — no per-pivot fix-up needed.

    Args:
        long: The long (play, participant) frame from ``_build_long_frame``.
        max_fetches: Hard cap on the number of HTTP requests issued. If
            the unique-athlete count exceeds this, a warning is logged
            and the surplus athletes are left with null names.
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        The same frame with ``player_name`` filled in-place where the
        ``$ref`` resolved cleanly. Failures (404, timeout, etc.) are
        logged and swallowed; the affected athlete keeps its null name
        and downstream regex fallbacks have a chance to recover.
    """
    if long.is_empty() or "player_name" not in long.columns:
        return long

    missing = (
        long.filter(
            pl.col("athlete_id").is_not_null() & pl.col("player_name").is_null() & pl.col("athlete_ref").is_not_null(),
        )
        .select(["athlete_id", "athlete_ref"])
        .unique(subset=["athlete_id"], keep="first", maintain_order=True)
    )
    if missing.is_empty():
        return long

    if missing.height > max_fetches:
        logger.warning(
            "cfb_play_participants: %d unique athletes need $ref resolution but "
            "max_fetches=%d; the first %d will be resolved and the remaining "
            "%d will retain null names.",
            missing.height,
            max_fetches,
            max_fetches,
            missing.height - max_fetches,
        )
        missing = missing.head(max_fetches)

    resolved: dict[str, str] = {}
    for row in missing.iter_rows(named=True):
        ath_id = row["athlete_id"]
        ref_url = row["athlete_ref"]
        try:
            resp = download(ref_url, **kwargs)
            payload = resp.json()
        except Exception as e:  # noqa: BLE001 — best-effort; log and continue
            logger.warning(
                "cfb_play_participants: failed to resolve athlete %s via %s: %s",
                ath_id,
                ref_url,
                e,
            )
            continue
        display = payload.get("displayName") or payload.get("fullName")
        if display:
            resolved[str(ath_id)] = str(display)

    if not resolved:
        return long

    return long.with_columns(
        player_name=pl.when(pl.col("player_name").is_null())
        .then(pl.col("athlete_id").replace_strict(resolved, default=None))
        .otherwise(pl.col("player_name")),
    )


def _pivot_wide(long: pl.DataFrame) -> pl.DataFrame:
    """Pivot the long (play, participant) frame to wide ``{type}_player_*`` columns.

    Emits two parallel column families for each participant ``type``:

    * **Scalar** (backwards-compatible) — ``{type}_player_name`` /
      ``{type}_player_id``: first occurrence of the type on the play.
    * **List** (additive) — ``{type}_player_names`` /
      ``{type}_player_ids``: ``List(Utf8)`` columns carrying every
      occurrence in ESPN-supplied order. Plays with no participant of a
      given type carry an empty list (``[]``) rather than null so
      downstream ``list.get(i, null_on_oob=True)`` reads stay simple.

    The list family closes the architectural gap that previously forced
    ``cfb_pbp.__add_player_cols`` to recover the second sacker on split
    sacks via a regex against ``cleaned_text``: ESPN ships the second
    ``sackedBy`` entry in the participants payload, but the scalar
    pivot collapses (play_id, type) duplicates with ``first`` for shape
    parity. The list pivot retains the full sequence.
    """
    fixed_cols = {"game_id", "play_id"}

    # ----- Scalar pivot (first-occurrence only) ---------------------------
    # Drop duplicate (play, type) rows so pivot's first-aggregation is
    # deterministic. polars 1.x ``pivot`` requires an aggregate function
    # when multiple rows map to the same (index, column) cell.
    deduped = long.unique(subset=["play_id", "participant_type"], keep="first", maintain_order=True)

    name_wide = deduped.pivot(
        on="participant_type",
        index=["game_id", "play_id"],
        values="player_name",
        aggregate_function="first",
    )
    id_wide = deduped.pivot(
        on="participant_type",
        index=["game_id", "play_id"],
        values="athlete_id",
        aggregate_function="first",
    )
    name_wide = name_wide.rename({c: f"{c}_player_name" for c in name_wide.columns if c not in fixed_cols})
    id_wide = id_wide.rename({c: f"{c}_player_id" for c in id_wide.columns if c not in fixed_cols})

    # ----- List pivot (every occurrence per play, in order) ---------------
    # Aggregate the long frame into one (play_id, type) row carrying the
    # full ordered list of names / ids for that type, then pivot the
    # list-typed values up into wide columns. polars 1.x ``pivot``
    # accepts list-typed ``values`` cleanly.
    grouped = long.group_by(["game_id", "play_id", "participant_type"], maintain_order=True).agg(
        pl.col("player_name").alias("name_list"),
        pl.col("athlete_id").alias("id_list"),
    )
    names_wide_list = grouped.pivot(
        on="participant_type",
        index=["game_id", "play_id"],
        values="name_list",
        aggregate_function="first",
    )
    ids_wide_list = grouped.pivot(
        on="participant_type",
        index=["game_id", "play_id"],
        values="id_list",
        aggregate_function="first",
    )
    names_wide_list = names_wide_list.rename(
        {c: f"{c}_player_names" for c in names_wide_list.columns if c not in fixed_cols},
    )
    ids_wide_list = ids_wide_list.rename({c: f"{c}_player_ids" for c in ids_wide_list.columns if c not in fixed_cols})
    # For plays that have no participant of a given type the pivot emits
    # null. Replace with empty list so downstream ``list.get(...)`` /
    # ``list.len()`` work without per-call null guards.
    names_wide_list = names_wide_list.with_columns(
        [
            pl.col(c).fill_null(pl.lit([], dtype=pl.List(pl.Utf8)))
            for c in names_wide_list.columns
            if c not in fixed_cols
        ],
    )
    ids_wide_list = ids_wide_list.with_columns(
        [pl.col(c).fill_null(pl.lit([], dtype=pl.List(pl.Utf8))) for c in ids_wide_list.columns if c not in fixed_cols],
    )

    # ----- Join all four pivots -------------------------------------------
    wide: pl.DataFrame = (
        name_wide.join(id_wide, on=["game_id", "play_id"], how="full", coalesce=True)
        .join(names_wide_list, on=["game_id", "play_id"], how="full", coalesce=True)
        .join(ids_wide_list, on=["game_id", "play_id"], how="full", coalesce=True)
    )
    # Cast play_id to Int64 to match the cfb_pbp ``id`` column dtype on the
    # join target. ESPN play ids are large but always numeric.
    wide = wide.with_columns(pl.col("play_id").cast(pl.Int64, strict=False))
    return wide
