"""Basketball id map: ESPN game / team / player ids <-> the alternate source's ids (private, experimental).

BB-D14 of the GOP basketball roadmap (spec ``2026-11-03-gop-basketball-impact-lineups-design.md`` §9). The
source adapters (BB-D13 onward) parse a non-ESPN feed and must rewrite its ids into ESPN's before the game page
or the win-probability model sees them; this module is that rewrite table and its lookup, one entry point per
league (:func:`_nba_idmap`, :func:`_wnba_idmap`, :func:`_mbb_idmap`, :func:`_wbb_idmap`).

Nothing is matched here. Every pair comes from a crosswalk that already exists:

| league | source id space | game leg | team leg | player leg |
|---|---|---|---|---|
| nba / wnba | ``stats`` -- stats.nba.com / stats.wnba.com, and cdn.nba.com / cdn.wnba.com live, which use the same game, team and person ids | ``{lg}_schedule_crosswalk`` | ``{lg}_team_crosswalk`` | ``{lg}_player_crosswalk`` (0.92 Jaro-Winkler name match, see below) |
| mbb / wbb | ``ncaa`` -- stats.ncaa.org | ``ncaa_{lg}_team_box`` (``contest_id`` <-> ``espn_game_id``, played games only) | ``ncaa_espn_team_crosswalk`` (season-keyed; NCAA team ids change every season) | **none**: no NCAA <-> ESPN player crosswalk exists yet |

The player crosswalks pair players with
:func:`~sportsdataverse._common_crosswalk_basketball.fuzzy_match` (exact normalized name, jersey tie-break, then
Jaro-Winkler at 0.92) inside each team or (game, team) block. That rule and its threshold are kept as they are;
:func:`_build_idmap` adds two things on top:

* **No silent drops.** An ESPN athlete the crosswalk could not pair stays a row with a null ``source_id``; a
  source player that no crosswalk row claims (pass ``source_players``) becomes a row with a null ``espn_id``.
  Both read ``match_method == "unmatched"``.
* **Reviewed overrides** (``overrides``): pairs a human checked, which win over the name match
  (``match_method == "override"``).

Every id is ``Utf8`` (labels, never arithmetic) and goes through
:func:`~sportsdataverse._common_crosswalk_basketball.str_id`, so a float-typed id (``1628932.0`` after a pandas
round trip) becomes ``"1628932"``, never ``"1628932.0"``; a ``".0"`` id that still reaches the output raises.

The nightly sdv-db job builds the same rows into ``{league}.source_idmap`` and serves them from
``GET /v1/{league}/idmap/{id}?kind=game|team|player&from=espn|source`` (:data:`IDMAP_ROUTE`,
:func:`_fetch_idmap_row`).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from functools import partial
from typing import Any, Optional

import polars as pl

from sportsdataverse._codegen_runtime import _as_season_list, _json_body, _transport_errors, _where
from sportsdataverse._common_crosswalk_basketball import str_id
from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.football.sources.idmap import _api_headers

IDMAP_SCHEMA: dict[str, pl.DataType] = {
    "league": pl.Utf8,
    "kind": pl.Utf8,  # "game" | "team" | "player"
    "season": pl.Int32,  # end year: 2026 = 2025-26
    "espn_id": pl.Utf8,  # null = a source id no crosswalk row claims
    "source": pl.Utf8,  # "stats" (nba/wnba stats + cdn live) | "ncaa"
    "source_id": pl.Utf8,  # null = an ESPN id the crosswalk could not pair
    "espn_team_id": pl.Utf8,  # player rows: the ESPN team of that row; null otherwise
    "name": pl.Utf8,  # ESPN's team / player display name; the source's name on a source-only player row
    "match_method": pl.Utf8,  # the crosswalk's method, "unmatched" or "override"
    "match_confidence": pl.Float64,
    "built_at": pl.Utf8,
}
"""One row = one id pair (or one explicitly unmatched id) = one sdv-db ``{league}.source_idmap`` row."""

KINDS = ("game", "team", "player")

SOURCE: dict[str, str] = {"nba": "stats", "wnba": "stats", "mbb": "ncaa", "wbb": "ncaa"}
"""The alternate id space per league. NBA/WNBA stats and cdn live share game, team and person ids."""

#: ``league -> kind -> (espn id column, source id column, name column, ESPN team column)`` of the input frame.
_LEGS: dict[str, dict[str, tuple[str, str, Optional[str], Optional[str]]]] = {
    **{
        lg: {
            "game": ("espn_game_id", f"{lg}_game_id", None, None),
            "team": ("espn_team_id", f"{lg}_team_id", "espn_display_name", None),
            "player": ("espn_athlete_id", f"{lg}_player_id", "espn_full_name", "espn_team_id"),
        }
        for lg in ("nba", "wnba")
    },
    **{
        lg: {
            "game": ("espn_game_id", "contest_id", None, None),
            "team": ("espn_team_id", "ncaa_team_id", "espn_display_name", None),
        }
        for lg in ("mbb", "wbb")
    },
}

IDMAP_ROUTE = "/v1/{league}/idmap/{id}"
"""Data API route (sdv-db ``api/idmap_routes.py``); ``kind`` and ``from`` are query parameters."""

_ID_COLUMNS = ("espn_id", "source_id", "espn_team_id")


def _check_league(league: str) -> None:
    if league not in SOURCE:
        raise ValueError(f"unknown league {league!r}; expected one of {sorted(SOURCE)}")


def _season(frame: pl.DataFrame) -> pl.Expr:
    """``season`` as Int32 end year; the NCAA crosswalk's ``"2025-26"`` text becomes 2026, ``"2026"`` stays 2026."""
    if frame.schema["season"] == pl.Utf8:
        span = pl.col("season").str.slice(0, 4).cast(pl.Int32) + 1
        return (
            pl.when(pl.col("season").str.contains("-")).then(span).otherwise(pl.col("season").cast(pl.Int32))
        ).alias("season")
    return pl.col("season").cast(pl.Int32)


def _leg(league: str, kind: str, frame: pl.DataFrame) -> pl.DataFrame:
    """Project one crosswalk frame onto :data:`IDMAP_SCHEMA` (ids normalized, nothing dropped)."""
    espn_col, source_col, name_col, team_col = _LEGS[league][kind]
    missing = [c for c in ("season", espn_col, source_col) if c not in frame.columns]
    if missing:
        raise ValueError(f"{league} {kind} frame has no column {missing} (columns: {frame.columns})")
    null = pl.lit(None, dtype=pl.Utf8)
    method = pl.col("match_method") if "match_method" in frame.columns else null
    conf = pl.col("match_confidence") if "match_confidence" in frame.columns else pl.lit(None, dtype=pl.Float64)
    return frame.select(
        pl.lit(kind).alias("kind"),
        _season(frame),
        str_id(frame, espn_col).alias("espn_id"),
        str_id(frame, source_col).alias("source_id"),
        (str_id(frame, team_col) if team_col else null).alias("espn_team_id"),
        (pl.col(name_col).cast(pl.Utf8) if name_col and name_col in frame.columns else null).alias("name"),
        method.cast(pl.Utf8).fill_null("crosswalk").alias("match_method"),
        conf.cast(pl.Float64).alias("match_confidence"),
    )


def _build_idmap(
    league: str,
    *,
    games: Optional[pl.DataFrame] = None,
    teams: Optional[pl.DataFrame] = None,
    players: Optional[pl.DataFrame] = None,
    source_players: Optional[pl.DataFrame] = None,
    overrides: Optional[pl.DataFrame] = None,
    built_at: Optional[str] = None,
) -> pl.DataFrame:
    """Build one league's id map from the crosswalk frames you already have (offline, no network).

    Args:
        league: ``nba``, ``wnba``, ``mbb`` or ``wbb``.
        games: NBA/WNBA: ``load_{lg}_schedule_crosswalk`` (``espn_game_id`` / ``{lg}_game_id``). MBB/WBB:
            ``load_ncaa_{lg}_team_box`` (``espn_game_id`` / ``contest_id``; one row per team, deduplicated).
        teams: NBA/WNBA: ``load_{lg}_team_crosswalk``. MBB/WBB: ``ncaa_espn_team_crosswalk(league)``.
        players: NBA/WNBA only: ``load_{lg}_player_crosswalk``. MBB/WBB has no player crosswalk; passing one
            raises.
        source_players: The source's own players (``season``, ``source_id``, ``name``), e.g. a stats player
            box. Each ``source_id`` that no ``players`` row claims (in any season) becomes an explicit
            unmatched row instead of disappearing.
        overrides: Reviewed pairs (``kind``, ``espn_id``, ``source_id``) that replace the name match for that
            ESPN id in every season; a source-only row for the same id is then dropped.
        built_at: ISO-8601 UTC stamp; defaults to now.

    Returns:
        A frame in :data:`IDMAP_SCHEMA`, one row per distinct (kind, season, espn_id, source_id, espn_team_id).

    Raises:
        ValueError: An unknown league, a missing column, a player frame for a college league, or an id that
            still reads like a float (``"123.0"``).
    """
    _check_league(league)
    built_at = built_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    frames = []
    for kind, frame in (("game", games), ("team", teams), ("player", players)):
        if frame is None:
            continue
        if kind not in _LEGS[league]:
            raise ValueError(f"{league} has no {kind} crosswalk (the NCAA <-> ESPN player leg does not exist yet)")
        frames.append(_leg(league, kind, frame))
    leg_cols = ("kind", "season", "espn_id", "source_id", "espn_team_id", "name", "match_method", "match_confidence")
    out = pl.concat(frames) if frames else pl.DataFrame(schema={c: IDMAP_SCHEMA[c] for c in leg_cols})

    if source_players is not None and source_players.height:
        src = source_players.select(
            pl.lit("player").alias("kind"),
            _season(source_players),
            pl.lit(None, dtype=pl.Utf8).alias("espn_id"),
            str_id(source_players, "source_id"),
            pl.lit(None, dtype=pl.Utf8).alias("espn_team_id"),
            pl.col("name").cast(pl.Utf8),
            pl.lit("unmatched").alias("match_method"),
            pl.lit(None, dtype=pl.Float64).alias("match_confidence"),
        ).unique(["season", "source_id"], keep="first", maintain_order=True)
        claimed = out.filter((pl.col("kind") == "player") & pl.col("source_id").is_not_null()).select("source_id")
        assert src.schema["source_id"] == claimed.schema["source_id"], "source_id dtype mismatch"
        out = pl.concat([out, src.join(claimed.unique(), on="source_id", how="anti")])

    if overrides is not None and overrides.height:
        ov = overrides.select(
            pl.col("kind").cast(pl.Utf8), str_id(overrides, "espn_id"), str_id(overrides, "source_id").alias("_ov")
        )
        clash = ov.group_by("kind", "espn_id").agg(pl.col("_ov").n_unique().alias("n")).filter(pl.col("n") > 1)
        if clash.height:
            raise ValueError(f"overrides pair one espn_id with several source_ids: {clash.head(3).rows()}")
        ov = ov.unique(["kind", "espn_id"], keep="last")
        clash = ov.group_by("kind", "_ov").agg(pl.col("espn_id").n_unique().alias("n")).filter(pl.col("n") > 1)
        if clash.height:
            raise ValueError(f"overrides pair one source_id with several espn_ids: {clash.head(3).rows()}")
        for k in ("kind", "espn_id"):
            assert out.schema[k] == ov.schema[k], f"{k} dtype mismatch"
        # The source ids an override takes off an ESPN row; kept below if nothing else claims them.
        displaced = (
            out.join(ov, on=["kind", "espn_id"], how="inner")
            .filter(pl.col("source_id").is_not_null() & (pl.col("source_id") != pl.col("_ov")))
            .select(
                "kind",
                "season",
                pl.lit(None, dtype=pl.Utf8).alias("espn_id"),
                "source_id",
                pl.lit(None, dtype=pl.Utf8).alias("espn_team_id"),
                pl.lit(None, dtype=pl.Utf8).alias("name"),
                pl.lit("unmatched").alias("match_method"),
                pl.lit(None, dtype=pl.Float64).alias("match_confidence"),
            )
        )
        out = (
            out.join(ov, on=["kind", "espn_id"], how="left")
            .with_columns(
                pl.coalesce("_ov", "source_id").alias("source_id"),
                pl.when(pl.col("_ov").is_not_null())
                .then(pl.lit("override"))
                .otherwise("match_method")
                .alias("match_method"),
                pl.when(pl.col("_ov").is_not_null())
                .then(pl.lit(1.0))
                .otherwise("match_confidence")
                .alias("match_confidence"),
            )
            .drop("_ov")
        )
        # Retire a source-only row only when its override actually landed on an ESPN row.
        landed = out.filter(pl.col("match_method") == "override").select("kind", "espn_id").unique()
        resolved = ov.join(landed, on=["kind", "espn_id"], how="semi").select("kind", pl.col("_ov").alias("source_id"))
        assert out.schema["source_id"] == resolved.schema["source_id"], "source_id dtype mismatch"
        out = pl.concat(
            [
                out.filter(pl.col("espn_id").is_not_null()),
                out.filter(pl.col("espn_id").is_null()).join(resolved, on=["kind", "source_id"], how="anti"),
            ]
        )
        claimed_now = out.filter(pl.col("source_id").is_not_null()).select("kind", "source_id").unique()
        assert displaced.schema["source_id"] == claimed_now.schema["source_id"], "source_id dtype mismatch"
        out = pl.concat([out, displaced.join(claimed_now, on=["kind", "source_id"], how="anti")])

    out = (
        out.filter(pl.col("espn_id").is_not_null() | pl.col("source_id").is_not_null())
        .with_columns(
            pl.when(pl.col("espn_id").is_null() | pl.col("source_id").is_null())
            .then(pl.lit("unmatched"))
            .otherwise("match_method")
            .alias("match_method")
        )
        .unique(["kind", "season", "espn_id", "source_id", "espn_team_id"], keep="first", maintain_order=True)
        .with_columns(
            pl.lit(league).alias("league"), pl.lit(SOURCE[league]).alias("source"), pl.lit(built_at).alias("built_at")
        )
        .select([pl.col(c).cast(t) for c, t in IDMAP_SCHEMA.items()])
    )
    floaty = out.filter(pl.any_horizontal(pl.col(c).str.ends_with(".0") for c in _ID_COLUMNS))
    if floaty.height:
        raise ValueError(f"float-origin id in the {league} id map: {floaty.head(3).rows()}")
    return out


def _id_text(value: Any) -> str:
    """One id as the map stores it: ``401809234``, ``401809234.0`` and ``"401809234"`` all read ``"401809234"``."""
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError("an id is required")
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{value!r} is not an id")
        return str(int(value))
    return str(value).strip()


def _check_query(kind: str, frm: str) -> None:
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {KINDS}, got {kind!r}")
    if frm not in ("espn", "source"):
        raise ValueError(f"frm must be 'espn' or 'source', got {frm!r}")


def _lookup(idmap: pl.DataFrame, kind: str, value: Any, *, frm: str = "espn") -> Optional[dict]:
    """The id-map row for one id, read in either direction.

    Args:
        idmap: A frame in :data:`IDMAP_SCHEMA` (one league, or several filtered by the caller).
        kind: ``game``, ``team`` or ``player``.
        value: The id to look up (int, str or an integer-valued float).
        frm: ``espn`` when ``value`` is an ESPN id, ``source`` when it is the source's id.

    Returns:
        The row as a dict, latest season first and a paired row before an unmatched one; None when the id is
        not in the map at all. A row whose other id is null means "seen, but unmatched", not "unknown".
    """
    _check_query(kind, frm)
    col, other = ("espn_id", "source_id") if frm == "espn" else ("source_id", "espn_id")
    hit = idmap.filter((pl.col("kind") == kind) & (pl.col(col) == _id_text(value))).sort(
        # Latest season, then a paired row, then a reviewed override over a name match for the same id.
        [pl.col("season"), pl.col(other).is_not_null(), pl.col("match_method") == "override"],
        descending=True,
        nulls_last=True,
    )
    return hit.row(0, named=True) if hit.height else None


def _fetch_idmap_row(
    league: str,
    value: Any,
    *,
    kind: str = "game",
    frm: str = "espn",
    base_url: str,
    transport: Callable[..., Any] = download,
    **kwargs: Any,
) -> Optional[dict]:
    """Read one id-map row from the Data API (``{base_url}/v1/{league}/idmap/{id}?kind=&from=``).

    Returns:
        The row as a dict (the :data:`IDMAP_SCHEMA` columns), or None on a 404 or an empty object.

    Raises:
        ValueError: An unknown league / kind / direction, or the Data API answered 400 / 422.
        AssetFetchError: Any other non-2xx, a connection failure after retries, or a body that is not a
            JSON object.
    """
    _check_league(league)
    _check_query(kind, frm)
    url = base_url.rstrip("/") + IDMAP_ROUTE.format(league=league, id=_id_text(value))
    kwargs.setdefault("headers", _api_headers())
    try:
        with _transport_errors(url):
            resp = transport(url=url, params={"kind": kind, "from": frm}, **kwargs)
        body = _json_body(resp, url)
    except NoDataError:
        return None
    if not isinstance(body, dict):
        raise AssetFetchError(f"{_where(url)} answered a JSON array, not an id-map row")
    return body or None


def _resolve(
    league: str,
    value: Any,
    *,
    kind: str = "game",
    frm: str = "espn",
    idmap: Optional[pl.DataFrame] = None,
    base_url: Optional[str] = None,
    **kwargs: Any,
) -> Optional[dict]:
    """Map one ``league`` id to the other id space: from a local ``idmap`` frame, else from the Data API.

    Args:
        league: ``nba``, ``wnba``, ``mbb`` or ``wbb`` (bound by the per-league entry points).
        value: The id to map.
        kind: ``game``, ``team`` or ``player``.
        frm: ``espn`` (``value`` is ESPN's; read ``source_id``) or ``source`` (read ``espn_id``).
        idmap: A frame from :func:`_build_idmap`; when given, no request is made.
        base_url: The Data API root, used when ``idmap`` is None.
        **kwargs: Passed to :func:`_fetch_idmap_row` (``transport``, ``headers``...).

    Returns:
        The id-map row (see :func:`_lookup`), or None when the id is not mapped.

    Raises:
        ValueError: Neither ``idmap`` nor ``base_url`` was given, or an argument is invalid.
    """
    _check_league(league)
    if idmap is not None:
        return _lookup(idmap.filter(pl.col("league") == league), kind, value, frm=frm)
    if base_url is None:
        raise ValueError("pass an idmap frame or the Data API base_url")
    return _fetch_idmap_row(league, value, kind=kind, frm=frm, base_url=base_url, **kwargs)


_nba_idmap = partial(_resolve, "nba")
_wnba_idmap = partial(_resolve, "wnba")
_mbb_idmap = partial(_resolve, "mbb")
_wbb_idmap = partial(_resolve, "wbb")


def _build_idmap_from_release(league: str, seasons: Any, **kwargs: Any) -> pl.DataFrame:
    """:func:`_build_idmap` over the published sportsdataverse-data assets (network).

    Args:
        league: ``nba``, ``wnba``, ``mbb`` or ``wbb``.
        seasons: Season end years, as the loaders take them (NBA/WNBA crosswalks start in 2026).
        **kwargs: ``source_players`` / ``overrides`` / ``built_at``, passed through.

    Returns:
        The id map frame (:data:`IDMAP_SCHEMA`).
    """
    _check_league(league)
    if league in ("nba", "wnba"):
        from importlib import import_module

        mod = import_module(f"sportsdataverse.{league}")
        return _build_idmap(
            league,
            games=getattr(mod, f"load_{league}_schedule_crosswalk")(seasons),
            teams=getattr(mod, f"load_{league}_team_crosswalk")(seasons),
            players=getattr(mod, f"load_{league}_player_crosswalk")(seasons),
            **kwargs,
        )
    from sportsdataverse.mbb import ncaa_espn_team_crosswalk

    if league == "mbb":
        from sportsdataverse.mbb import load_ncaa_mbb_team_box as box
    else:
        from sportsdataverse.wbb import load_ncaa_wbb_team_box as box
    wanted = sorted(set(_as_season_list(seasons)))  # "2026", 2026.0 and np.int64(2026) are one season
    teams = ncaa_espn_team_crosswalk(league)
    teams = teams.filter((pl.col("season").str.slice(0, 4).cast(pl.Int32) + 1).is_in(wanted))
    return _build_idmap(league, games=box(wanted), teams=teams, **kwargs)
