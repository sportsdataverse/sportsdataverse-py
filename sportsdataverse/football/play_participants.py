"""ESPN per-play participants, shared by the CFB and NFL play-by-play processors.

Single ESPN endpoint per league::

    sports.core.api.espn.com/v2/sports/football/leagues/{league}/events/{game_id}/competitions/{game_id}/plays?limit=1000

ESPN's per-play ``participants[]`` array is the authoritative source for which
athletes were involved in each play (passer, rusher, receiver, tackler, ...).
This module pulls the full play-list for a game, extracts the participants,
resolves each ``$ref`` URL into an ``athlete_id`` / ``position_id``, attaches
the per-athlete display name from the league's ``cdn.espn.com/core/{league}/playbyplay``
sidecar (with a capped per-athlete ``$ref`` fan-out for anyone the sidecar
omits), and pivots the result so each play has one row keyed by ``play_id`` with
``{type}_player_name`` / ``{type}_player_id`` columns.

:func:`sportsdataverse.cfb.cfb_play_participants.espn_cfb_play_participants`
and :func:`sportsdataverse.nfl.nfl_play_participants.espn_nfl_play_participants`
are the league entry points; :func:`coalesce_participants` is the step both
processors run to overwrite the text-extracted names with ESPN's.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, underscore

logger = logging.getLogger("sdv.football.play_participants")
logger.addHandler(logging.NullHandler())

__all__ = [
    "athlete_lookup_from_summary",
    "coalesce_participants",
    "espn_play_participants",
    "play_participants_from_items",
]

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


def espn_play_participants(
    game_id: int,
    *,
    league: str = "college-football",
    raw: bool = False,
    return_as_pandas: bool = False,
    resolve_missing: bool = True,
    resolve_missing_max: int = 50,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull ESPN per-play participants for a football game.

    ``league`` is the ESPN league segment: ``"college-football"`` or ``"nfl"``.

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
        sportsdataverse.errors.NoDataError: ESPN returned 404.
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
    plays_raw = _download_plays(game_id, league=league, **kwargs)

    if raw:
        return {"items": plays_raw}

    athlete_lookup = _download_athlete_lookup(game_id, league=league, **kwargs)

    long = _build_long_frame(plays_raw, athlete_lookup, game_id)

    if long.is_empty():
        empty = pl.DataFrame(schema=_EMPTY_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    if resolve_missing:
        long = _resolve_missing_athletes(long, max_fetches=resolve_missing_max, **kwargs)

    wide = _pivot_wide(long)

    return wide.to_pandas() if return_as_pandas else wide


def play_participants_from_items(
    plays_raw: list[dict[str, Any]],
    game_id: int,
    *,
    athlete_lookup: dict[str, str] | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Build the wide participants frame from already-fetched play items, no network.

    The offline twin of :func:`espn_play_participants` for a committed raw
    library: ``plays_raw`` is the core plays ``items`` list (each play needs
    only ``id`` and ``participants[]`` with athlete / position ``$ref``, so a
    slimmed capture works), ``athlete_lookup`` an ``athlete_id -> display name``
    map -- see :func:`athlete_lookup_from_summary`. Names left unresolved stay
    null, which :func:`coalesce_participants` treats as "keep the text name".

    Args:
        plays_raw: The plays ``items`` list from ESPN's core plays endpoint.
        game_id: ESPN game / event identifier stamped on every row.
        athlete_lookup: ``athlete_id -> display name``; ``{}`` yields id-only rows.
        return_as_pandas: If True, returns a pandas DataFrame; otherwise polars.

    Returns:
        The same wide one-row-per-play frame :func:`espn_play_participants` returns.

    Example:
        From a stored game::

            from sportsdataverse.football.play_participants import (
                athlete_lookup_from_summary,
                play_participants_from_items,
            )
            parts = play_participants_from_items(
                plays["items"], 401872922, athlete_lookup=athlete_lookup_from_summary(summary)
            )
    """
    long = _build_long_frame(plays_raw, athlete_lookup or {}, game_id)
    if long.is_empty():
        empty = pl.DataFrame(schema=_EMPTY_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty
    wide = _pivot_wide(long)
    return wide.to_pandas() if return_as_pandas else wide


def athlete_lookup_from_summary(summary: dict[str, Any]) -> dict[str, str]:
    """``athlete_id -> display name`` from a summary's ``boxscore.players``.

    An offline stand-in for the ``cdn.espn.com`` playbyplay sidecar: every
    athlete with a stat line in the game. Athletes with no stat line (a
    penalized lineman, say) are absent and their names stay unresolved.

    Args:
        summary: An ESPN game summary payload (``site.api.espn.com`` ``summary``).

    Returns:
        Dict of athlete id (str) to display name.

    Example:
        Resolve names for a stored game::

            lookup = athlete_lookup_from_summary(summary)
            lookup.get("4241478")
    """
    lookup: dict[str, str] = {}
    box = summary.get("boxscore") if isinstance(summary, dict) else None
    for team in (box or {}).get("players") or []:
        for stat in (team or {}).get("statistics") or []:
            for row in (stat or {}).get("athletes") or []:
                ath = (row or {}).get("athlete") or {}
                aid = ath.get("id")
                name = ath.get("displayName") or ath.get("fullName")
                if aid is not None and name:
                    lookup[str(aid)] = str(name)
    return lookup


def _download_plays(game_id: int, league: str = "college-football", **kwargs: Any) -> list[dict[str, Any]]:
    """Pull every play dict for ``game_id``, following ``pageCount``."""
    base = (
        f"https://sports.core.api.espn.com/v2/sports/football/leagues/{league}/"
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


def _download_athlete_lookup(game_id: int, league: str = "college-football", **kwargs: Any) -> dict[str, str]:
    """Build an ``athlete_id -> display_name`` map from the playbyplay sidecar.

    Uses the ``cdn.espn.com/core/college-football/playbyplay`` XHR endpoint
    that the legacy class already relies on for the box-score side; the
    payload carries every athlete that appears in the game, keyed by id.
    """
    url = f"https://cdn.espn.com/core/{league}/playbyplay?xhr=1&gameId={game_id}"
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
            "football.play_participants: %d unique athletes need $ref resolution but "
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
                "football.play_participants: failed to resolve athlete %s via %s: %s",
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
    * **Position** — ``{type}_position_id``: the ESPN position id of the
      first participant of the type (``sportsdataverse.football.positions``
      maps it to an abbreviation and a position group).
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
    # position of the first participant of each type (ESPN position id; see
    # sportsdataverse.football.positions for the id -> group map) -- what the
    # usage box needs to split tackles and first downs by position group
    pos_wide = deduped.pivot(
        on="participant_type",
        index=["game_id", "play_id"],
        values="position_id",
        aggregate_function="first",
    )
    name_wide = name_wide.rename({c: f"{c}_player_name" for c in name_wide.columns if c not in fixed_cols})
    id_wide = id_wide.rename({c: f"{c}_player_id" for c in id_wide.columns if c not in fixed_cols})
    pos_wide = pos_wide.rename({c: f"{c}_position_id" for c in pos_wide.columns if c not in fixed_cols})

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
        .join(pos_wide, on=["game_id", "play_id"], how="full", coalesce=True)
        .join(names_wide_list, on=["game_id", "play_id"], how="full", coalesce=True)
        .join(ids_wide_list, on=["game_id", "play_id"], how="full", coalesce=True)
    )
    # Cast play_id to Int64 to match the cfb_pbp ``id`` column dtype on the
    # join target. ESPN play ids are large but always numeric.
    wide = wide.with_columns(pl.col("play_id").cast(pl.Int64, strict=False))
    return wide


# Participant type -> the play-by-play name column it overwrites; ``True`` fills a
# null pbp name from the participant, ``False`` only overwrites an existing one.
_COALESCE_PAIRS = [
    ("passer_player_name", "passer_player_name_part", True),
    ("rusher_player_name", "rusher_player_name_part", True),
    ("receiver_player_name", "receiver_player_name_part", True),
    ("punter_player_name", "punter_player_name_part", True),
    ("fg_kicker_player_name", "kicker_player_name_part", True),
    ("sack_player_name", "sacked_by_player_name_part", True),
    ("fumble_forced_player_name", "forced_by_player_name_part", True),
    ("fumble_recovered_player_name", "recoverer_player_name_part", True),
    ("punt_return_player_name", "returner_player_name_part", False),
    ("kickoff_return_player_name", "returner_player_name_part", False),
]
_PARTICIPANT_NAME_COLS = [
    "kicker_player_name",
    "returner_player_name",
    "passer_player_name",
    "receiver_player_name",
    "rusher_player_name",
    "punter_player_name",
    "pass_defender_player_name",
    "sacked_by_player_name",
    "forced_by_player_name",
    "recoverer_player_name",
]
_ID_PAIRS = [
    ("passer_player_id", "passer_player_id_part"),
    ("rusher_player_id", "rusher_player_id_part"),
    ("receiver_player_id", "receiver_player_id_part"),
    ("punter_player_id", "punter_player_id_part"),
    ("fg_kicker_player_id", "kicker_player_id_part"),
    ("sack_player_id", "sacked_by_player_id_part"),
    ("fumble_forced_player_id", "forced_by_player_id_part"),
    ("fumble_recovered_player_id", "recoverer_player_id_part"),
    ("punt_return_player_id", "returner_player_id_part"),
    ("kickoff_return_player_id", "returner_player_id_part"),
    ("interception_player_id", "pass_defender_player_id_part"),
    ("pass_breakup_player_id", "pass_defender_player_id_part"),
]


def coalesce_participants(play_df: pl.DataFrame, parts: pl.DataFrame, *, prefer_ids: bool = False) -> pl.DataFrame:
    """Overwrite the regex-extracted player names with ESPN's participant names.

    Joins ``parts`` (the wide frame :func:`espn_play_participants` returns) onto
    ``play_df`` by play id and coalesces ESPN's display name over each
    text-extracted ``*_player_name`` wherever the participant name is non-null;
    ESPN files the interceptor as the pass defender, so that column feeds
    ``interception_player_name`` on interceptions and ``pass_breakup_player_name``
    otherwise. With ``prefer_ids`` the participant ``athlete_id`` also takes
    precedence over any existing ``*_player_id`` (the NFL path, whose ids are
    otherwise resolved from the box score). Returns ``play_df`` unchanged when
    ``parts`` is empty or the frame carries no ``id``.
    """
    if parts is None or parts.height == 0 or "id" not in play_df.columns:
        return play_df
    available_part_cols = [c for c in _PARTICIPANT_NAME_COLS if c in parts.columns]
    id_part_cols = [c.replace("_name", "_id") for c in available_part_cols] if prefer_ids else []
    id_part_cols = [c for c in id_part_cols if c in parts.columns]
    take = ["play_id", *available_part_cols, *id_part_cols]
    parts_slim = parts.select(take).rename({c: f"{c}_part" for c in available_part_cols + id_part_cols})
    play_df = play_df.join(parts_slim, how="left", left_on="id", right_on="play_id")
    coalesce_exprs = []
    for pbp_col, part_col, populate_if_null in _COALESCE_PAIRS:
        if pbp_col in play_df.columns and part_col in play_df.columns:
            if populate_if_null:
                expr = pl.coalesce(pl.col(part_col).str.strip_chars(), pl.col(pbp_col)).alias(pbp_col)
            else:
                expr = (
                    pl.when(pl.col(pbp_col).is_not_null() & pl.col(part_col).is_not_null())
                    .then(pl.col(part_col).str.strip_chars())
                    .otherwise(pl.col(pbp_col))
                    .alias(pbp_col)
                )
            coalesce_exprs.append(expr)
    pd_part = "pass_defender_player_name_part"
    if pd_part in play_df.columns:
        is_int = (pl.col("int") == True) if "int" in play_df.columns else pl.lit(False)  # noqa: E712
        if "interception_player_name" in play_df.columns:
            coalesce_exprs.append(
                pl.when(is_int)
                .then(pl.coalesce(pl.col(pd_part).str.strip_chars(), pl.col("interception_player_name")))
                .otherwise(pl.col("interception_player_name"))
                .alias("interception_player_name"),
            )
        if "pass_breakup_player_name" in play_df.columns:
            coalesce_exprs.append(
                pl.when(is_int)
                .then(pl.col("pass_breakup_player_name"))
                .otherwise(pl.coalesce(pl.col(pd_part).str.strip_chars(), pl.col("pass_breakup_player_name")))
                .alias("pass_breakup_player_name"),
            )
    if prefer_ids and "sacked_by_player_names" in parts.columns and "sack_player_name2" in play_df.columns:
        # a split sack ships two ``sackedBy`` participants; the scalar column above
        # carries only the first, so the second name comes from the list family
        second = parts.select(
            pl.col("play_id"),
            pl.col("sacked_by_player_names").list.get(1, null_on_oob=True).alias("_sack2_part"),
            (
                pl.col("sacked_by_player_ids").list.get(1, null_on_oob=True).alias("_sack2_id_part")
                if "sacked_by_player_ids" in parts.columns
                else pl.lit(None, dtype=pl.Utf8).alias("_sack2_id_part")
            ),
        )
        play_df = play_df.join(second, how="left", left_on="id", right_on="play_id")
        coalesce_exprs.append(
            pl.coalesce(pl.col("_sack2_part").str.strip_chars(), pl.col("sack_player_name2")).alias(
                "sack_player_name2"
            ),
        )
        existing2 = (
            pl.col("sack_player_id2").cast(pl.Utf8)
            if "sack_player_id2" in play_df.columns
            else pl.lit(None, dtype=pl.Utf8)
        )
        coalesce_exprs.append(pl.coalesce(pl.col("_sack2_id_part").cast(pl.Utf8), existing2).alias("sack_player_id2"))
    if prefer_ids:
        is_int = (pl.col("int") == True) if "int" in play_df.columns else pl.lit(False)  # noqa: E712
        for id_col, part_col in _ID_PAIRS:
            if part_col not in play_df.columns:
                continue
            src = pl.col(part_col).cast(pl.Utf8)
            if id_col == "interception_player_id":
                src = pl.when(is_int).then(src).otherwise(None)
            elif id_col == "pass_breakup_player_id":
                src = pl.when(is_int).then(None).otherwise(src)
            existing = pl.col(id_col).cast(pl.Utf8) if id_col in play_df.columns else pl.lit(None, dtype=pl.Utf8)
            coalesce_exprs.append(pl.coalesce(src, existing).alias(id_col))
    if coalesce_exprs:
        play_df = play_df.with_columns(coalesce_exprs)
    drop = [f"{c}_part" for c in available_part_cols + id_part_cols] + ["_sack2_part", "_sack2_id_part"]
    return play_df.drop([c for c in drop if c in play_df.columns])
