"""SDV-native NFL players builder (public ESPN-athletes tier only) + crosswalk.

This module is the *self-sufficient* half of sdv-py's player-identity story. It
pairs an SDV-native **builder** (reached only through public ESPN endpoints) with
a pure-**consumer** crosswalk over nflverse's published players master:

* :func:`sportsdataverse.nfl.load_nfl_players` reads nflverse's **published**
  ``players.parquet`` — the union of **seven** upstream identity systems (GSIS,
  ESPN, NGS roster, PFR, OTC, PFF, Sleeper/Yahoo cross-walk). Three of those
  (PFR, OTC, PFF) require **private credentials** sdv-py does not hold, so that
  parquet is the only place the full seven-source master is available. Prefer it
  as the **identity master** whenever a round trip to nflverse is acceptable.
* :func:`build_nfl_players` rebuilds a players frame from **only the public
  ESPN NFL athletes endpoint** (``sports.core.api.espn.com/.../athletes``), which
  sdv-py can reach without credentials. It is therefore a *partial* mirror: it
  carries the dense ESPN-native identity fields (``espn_id``, name, position,
  jersey, height, weight, ``birth_date``, ``status``, ``headshot_url``) and a
  best-effort ``gsis_id`` (+ other cross-IDs) enriched from
  :func:`load_nfl_players`. ESPN-native rows with no nflverse match keep only
  their ESPN fields. Use it for **SDV-native self-sufficiency** (no nflverse
  release dependency).
* :func:`nfl_players_crosswalk` is a pure consumer of
  :func:`load_nfl_players` — it slices the published parquet down to just the
  ID-crosswalk columns (``gsis_id``, ``espn_id``, ``pfr_id``, …) plus
  ``full_name`` / ``position``, deduped on ``gsis_id``, as a convenience for
  joining nflverse IDs onto PBP / rosters / stats frames.

ESPN-ID dedup (critical): ESPN migrated ~4-digit -> ~7-digit athlete ids around
2007, so a handful of players carry two ESPN ids. :func:`build_nfl_players`
replicates nflverse's rule and keeps the **highest numeric ``espn_id`` per
``(full_name, birth_date)``** so downstream joins never duplicate.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional, Union, overload

import polars as pl

from sportsdataverse.dl_utils import download

if TYPE_CHECKING:  # pragma: no cover -- annotation-only import (PEP 563 defers eval)
    import pandas as pd

__all__ = ["build_nfl_players", "nfl_players_crosswalk"]

# ESPN public NFL athletes listing (paginated ``$ref`` index) + per-athlete detail.
_ATHLETES_URL = "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes"
_PAGE_LIMIT = 1000
# Bounded concurrency for resolving athlete ``$ref`` detail resources. ESPN's
# core v2 API rate-limits aggressive parallelism (403s), and ``download()``
# already retries with Retry-After backoff, so keep this modest.
_FETCH_WORKERS = 8

# ---------------------------------------------------------------------------
# SDV-native players schema. ESPN supplies the identity + physical fields
# densely; the cross-system IDs are best-effort enrichments joined from
# load_nfl_players() on (full_name, birth_date). Columns ESPN does not supply
# are still emitted (null / enriched) so the frame carries a stable, documented
# column set even when a fetch is empty.
# ---------------------------------------------------------------------------
_SCHEMA: Dict[str, pl.DataType] = {
    "espn_id": pl.Utf8,
    "full_name": pl.Utf8,
    "first_name": pl.Utf8,
    "last_name": pl.Utf8,
    "position": pl.Utf8,
    "team": pl.Utf8,
    "jersey": pl.Utf8,
    "height": pl.Float64,
    "weight": pl.Float64,
    "birth_date": pl.Utf8,
    "status": pl.Utf8,
    "headshot_url": pl.Utf8,
    "gsis_id": pl.Utf8,
    "esb_id": pl.Utf8,
    "pfr_id": pl.Utf8,
    "pff_id": pl.Utf8,
    "smart_id": pl.Utf8,
    "college": pl.Utf8,
}

# Cross-system ID + enrichment columns sourced from load_nfl_players() (joined
# on espn_id first, then a name+dob fallback). ESPN itself supplies none of
# these; left-null when unmatched. {target_col: players_table_col}.
_PLAYER_ENRICH: Dict[str, str] = {
    "gsis_id": "gsis_id",
    "esb_id": "esb_id",
    "pfr_id": "pfr_id",
    "pff_id": "pff_id",
    "smart_id": "smart_id",
    "college": "college_name",
}


def _empty_frame(return_as_pandas: bool) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Zero-row frame carrying the full documented schema (never raises)."""
    frame = pl.DataFrame(schema=_SCHEMA)
    return frame.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else frame


def _athlete_row(athlete: Dict) -> Optional[Dict]:
    """Map one ESPN athlete-detail payload onto the SDV-native players schema.

    Cross-system IDs (``gsis_id`` etc.) are left ``None`` here; they are filled
    by the :func:`load_nfl_players` enrichment join. Returns ``None`` for a
    payload with no usable ``id`` so the caller can skip it.
    """
    espn_id = athlete.get("id")
    if espn_id is None:
        return None
    position = athlete.get("position") or {}
    status = athlete.get("status") or {}
    headshot = athlete.get("headshot") or {}
    return {
        "espn_id": str(espn_id),
        "full_name": athlete.get("fullName") or athlete.get("displayName"),
        "first_name": athlete.get("firstName"),
        "last_name": athlete.get("lastName"),
        "position": position.get("abbreviation") if isinstance(position, dict) else None,
        # The ESPN core-v2 athlete detail exposes ``team`` only as a ``$ref`` URL;
        # the team abbreviation is not inlined, so ``team`` is left null here
        # (best-effort field — populate via a roster join if needed).
        "team": None,
        "jersey": athlete.get("jersey"),
        "height": athlete.get("height"),
        "weight": athlete.get("weight"),
        "birth_date": athlete.get("dateOfBirth"),
        "status": status.get("name") if isinstance(status, dict) else (status or None),
        "headshot_url": headshot.get("href") if isinstance(headshot, dict) else None,
        "gsis_id": None,
        "esb_id": None,
        "pfr_id": None,
        "pff_id": None,
        "smart_id": None,
        "college": None,
    }


def _fetch_athletes(limit: Optional[int] = None) -> List[Dict]:
    """Fetch ESPN's public NFL athletes and return a list of detail payloads.

    Walks the paginated ``$ref`` index at :data:`_ATHLETES_URL`, then resolves
    each athlete's detail resource (one round trip per athlete). ``limit`` caps
    the number of athletes resolved (``None`` = all). All HTTP goes through the
    package gateway :func:`sportsdataverse.dl_utils.download`; a failed page /
    athlete is skipped rather than raising, so a partial/empty fetch degrades to
    a smaller (or zero-row) frame.
    """
    refs: List[str] = []
    page = 1
    while True:
        resp = download(url=_ATHLETES_URL, params={"limit": _PAGE_LIMIT, "page": page, "active": "true"})
        payload = {}
        if resp is not None:
            try:
                payload = resp.json()
            except Exception:  # noqa: BLE001 -- malformed page degrades to empty
                payload = {}
        items = payload.get("items") or []
        for item in items:
            ref = item.get("$ref") if isinstance(item, dict) else None
            if ref:
                refs.append(ref)
                if limit is not None and len(refs) >= limit:
                    break
        page_count = payload.get("pageCount") or 0
        if (limit is not None and len(refs) >= limit) or page >= page_count or not items:
            break
        page += 1

    def _resolve(ref: str) -> Optional[Dict]:
        resp = download(url=ref)
        if resp is None:
            return None
        try:
            detail = resp.json()
        except Exception:  # noqa: BLE001 -- malformed athlete degrades to skip
            return None
        return detail if isinstance(detail, dict) else None

    athletes: List[Dict] = []
    if refs:
        from concurrent.futures import ThreadPoolExecutor

        # download() owns its own pooled session + Retry-After backoff, so a
        # bounded pool resolves the ~7.5k athlete $refs concurrently without
        # tripping ESPN's rate limit. ex.map preserves input order; failures
        # come back as None and are dropped.
        with ThreadPoolExecutor(max_workers=min(_FETCH_WORKERS, len(refs))) as ex:
            athletes = [d for d in ex.map(_resolve, refs) if d is not None]
    return athletes


def _dedup_espn_id(frame: pl.DataFrame) -> pl.DataFrame:
    """Keep the HIGHEST numeric ``espn_id`` per ``(full_name, birth_date)``.

    Replicates nflverse's ESPN-id reconciliation: ESPN's ~2007 migration from
    ~4-digit to ~7-digit athlete ids left some players with two ``espn_id``
    rows. Sorting by the numeric id descending and keeping the first row per
    ``(full_name, birth_date)`` selects the modern (higher) id so downstream
    joins never duplicate.
    """
    if frame.is_empty():
        return frame
    return (
        frame.with_columns(pl.col("espn_id").cast(pl.Int64, strict=False).alias("_espn_id_num"))
        .sort("_espn_id_num", descending=True, nulls_last=True)
        .unique(subset=["full_name", "birth_date"], keep="first", maintain_order=True)
        .drop("_espn_id_num")
    )


def _enrich_cross_ids(frame: pl.DataFrame) -> pl.DataFrame:
    """Left-join :func:`load_nfl_players` cross-system IDs + college.

    Best-effort: a failed players load (or a players frame missing the join
    keys) leaves the enrichment columns untouched (ESPN-native / null). The
    join only *fills* columns ESPN left null — it never overwrites an
    ESPN-supplied value. Matching is by ``espn_id`` first, then a
    ``(full_name, birth_date)`` fallback for rows the espn_id join missed.
    """
    from sportsdataverse.nfl.nfl_loaders import load_nfl_players

    try:
        players = load_nfl_players()
    except Exception:  # noqa: BLE001 -- enrichment is strictly best-effort
        return frame
    if players.is_empty():
        return frame

    have = [src for src in set(_PLAYER_ENRICH.values()) if src in players.columns]
    if not have:
        return frame

    # Normalise the players-table birth_date to a bare YYYY-MM-DD string so the
    # name+dob fallback lines up with ESPN's ``dateOfBirth`` (also normalised).
    name_col = "display_name" if "display_name" in players.columns else None

    def _norm_dob(col: str) -> pl.Expr:
        return pl.col(col).cast(pl.Utf8).str.slice(0, 10)

    frame = frame.with_columns(_norm_dob("birth_date").alias("_dob"))

    # --- Pass 1: join on espn_id (dense + unambiguous when present). ---
    if "espn_id" in players.columns:
        p1 = players.select(["espn_id", *have]).filter(pl.col("espn_id").is_not_null())
        p1 = p1.with_columns(pl.col("espn_id").cast(pl.Utf8)).unique(subset=["espn_id"], keep="first")
        p1 = p1.rename({src: f"_p1_{src}" for src in have})
        frame = frame.join(p1, on="espn_id", how="left")
    else:
        for src in have:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Utf8).alias(f"_p1_{src}"))

    # --- Pass 2: name+dob fallback for rows pass 1 missed. ---
    if name_col is not None and "birth_date" in players.columns:
        p2 = players.select([name_col, "birth_date", *have])
        p2 = p2.with_columns(_norm_dob("birth_date").alias("_dob")).drop("birth_date")
        p2 = p2.rename({name_col: "full_name"})
        p2 = p2.unique(subset=["full_name", "_dob"], keep="first")
        p2 = p2.rename({src: f"_p2_{src}" for src in have})
        frame = frame.join(p2, on=["full_name", "_dob"], how="left")
    else:
        for src in have:
            frame = frame.with_columns(pl.lit(None, dtype=pl.Utf8).alias(f"_p2_{src}"))

    fills = []
    for target, src in _PLAYER_ENRICH.items():
        p1c, p2c = f"_p1_{src}", f"_p2_{src}"
        if p1c in frame.columns or p2c in frame.columns:
            parts = [pl.col(target)]
            if p1c in frame.columns:
                parts.append(pl.col(p1c).cast(pl.Utf8))
            if p2c in frame.columns:
                parts.append(pl.col(p2c).cast(pl.Utf8))
            fills.append(pl.coalesce(parts).alias(target))
    if fills:
        frame = frame.with_columns(fills)
    drop = [c for c in frame.columns if c.startswith("_p1_") or c.startswith("_p2_") or c == "_dob"]
    return frame.drop(drop)


@overload
def build_nfl_players() -> pl.DataFrame: ...
@overload
def build_nfl_players(*, return_as_pandas: bool = ...) -> Union[pl.DataFrame, "pd.DataFrame"]: ...


def build_nfl_players(
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Build an SDV-native NFL players frame from ESPN's public athletes endpoint.

    Walks ESPN's public NFL athletes index
    (``sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes``),
    resolves each athlete's detail resource, flattens it onto the SDV-native
    players schema, **dedups to the highest numeric ``espn_id`` per
    ``(full_name, birth_date)``** (the ESPN ~2007 4-digit -> 7-digit id
    migration left some players with two ids), and enriches ``gsis_id`` +
    other cross-IDs by a best-effort join against
    :func:`sportsdataverse.nfl.load_nfl_players`.

    This is the **public ESPN-athletes tier only** — a partial mirror of
    nflverse's full seven-source ``players.parquet`` (three of those sources,
    PFR / OTC / PFF, require private credentials). ESPN-native rows with no
    nflverse match keep only their ESPN fields (cross-IDs left null). For the
    full identity master prefer :func:`sportsdataverse.nfl.load_nfl_players`;
    use :func:`build_nfl_players` when you need an SDV-native frame that depends
    only on the live public ESPN API.

    Args:
        return_as_pandas: If ``True``, return a ``pandas.DataFrame``; otherwise a
            ``polars.DataFrame`` (default).

    Returns:
        A one-row-per-player ``DataFrame`` with the documented schema
        (``espn_id``, ``full_name``, ``first_name``, ``last_name``,
        ``position``, ``team``, ``jersey``, ``height``, ``weight``,
        ``birth_date``, ``status``, ``headshot_url``, ``gsis_id``, ``esb_id``,
        ``pfr_id``, ``pff_id``, ``smart_id``, ``college``). An empty / failed
        fetch yields a zero-row frame carrying the same column set (never a
        raise).

    Example:
        Quick start::

            from sportsdataverse.nfl import build_nfl_players
            players = build_nfl_players()
            print(players.shape)

        Pandas output::

            df = build_nfl_players(return_as_pandas=True)

        Pipeline next step (one line)::

            import polars as pl
            build_nfl_players().filter(pl.col("position") == "QB").head()

        See Also:
            * `nflverse`_ -- full seven-source identity master (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings (load_players)

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    athletes = _fetch_athletes()
    rows = [row for athlete in athletes if (row := _athlete_row(athlete)) is not None]
    if not rows:
        return _empty_frame(return_as_pandas)

    frame = pl.DataFrame(rows, schema=_SCHEMA)
    frame = _dedup_espn_id(frame)
    frame = _enrich_cross_ids(frame)
    # Re-assert column order (the enrichment join can reorder).
    frame = frame.select(list(_SCHEMA.keys()))
    return frame.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else frame


# ID-crosswalk columns sliced from load_nfl_players(). Only the subset the
# published parquet actually carries is retained (whatever is missing is
# dropped), plus full_name + position for human-readable joins.
_CROSSWALK_IDS: List[str] = [
    "gsis_id",
    "esb_id",
    "espn_id",
    "pfr_id",
    "pff_id",
    "otc_id",
    "nfl_id",
    "smart_id",
]


@overload
def nfl_players_crosswalk() -> pl.DataFrame: ...
@overload
def nfl_players_crosswalk(*, return_as_pandas: bool = ...) -> Union[pl.DataFrame, "pd.DataFrame"]: ...


def nfl_players_crosswalk(
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Pure-consumer ID crosswalk sliced from :func:`load_nfl_players`.

    Reads nflverse's published players master and projects it down to just the
    cross-system identifier columns it carries (``gsis_id``, ``esb_id``,
    ``espn_id``, ``pfr_id``, ``pff_id``, ``otc_id``, ``nfl_id``, ``smart_id`` —
    whichever the parquet exposes) plus ``full_name`` and ``position``, deduped
    on ``gsis_id``. It is a convenience for joining nflverse identity IDs onto
    PBP / rosters / stats frames without carrying the full ~40-column master.

    Args:
        return_as_pandas: If ``True``, return a ``pandas.DataFrame``; otherwise a
            ``polars.DataFrame`` (default).

    Returns:
        A one-row-per-``gsis_id`` ``DataFrame`` of cross-system IDs +
        ``full_name`` / ``position``. A failed / empty players load yields a
        zero-row frame carrying the same column set (never a raise).

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_players_crosswalk
            xwalk = nfl_players_crosswalk()
            print(xwalk.columns)

        Join nflverse IDs onto a PBP frame (one line)::

            pbp.join(nfl_players_crosswalk(), left_on="passer_player_id", right_on="gsis_id", how="left")

        See Also:
            * `nflverse`_ -- full seven-source identity master (R + Python)
            * `nflreadpy`_ -- direct nflverse Python bindings (load_players)

        .. _nflverse: https://nflverse.nflverse.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    from sportsdataverse.nfl.nfl_loaders import load_nfl_players

    name_aliases = ["full_name", "display_name"]
    base_cols = ["full_name", "position", *_CROSSWALK_IDS]

    try:
        players = load_nfl_players()
    except Exception:  # noqa: BLE001 -- pure consumer; degrade to empty on failure
        players = pl.DataFrame()

    if players.is_empty():
        empty = pl.DataFrame(schema={c: pl.Utf8 for c in base_cols})
        return empty.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else empty

    # Resolve a full_name column (the parquet uses display_name).
    name_src = next((c for c in name_aliases if c in players.columns), None)
    if name_src is not None and name_src != "full_name":
        players = players.rename({name_src: "full_name"})

    keep = [c for c in base_cols if c in players.columns]
    frame = players.select(keep)
    if "gsis_id" in frame.columns:
        frame = frame.unique(subset=["gsis_id"], keep="first", maintain_order=True)
    return frame.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else frame
