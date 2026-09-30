"""NBA cdn.nba.com liveData play-by-play and boxscore wrappers.

``cdn.nba.com``/``cdn.wnba.com`` serve the same static liveData JSON that powers
NBA.com's live scoreboard: per-whistle referee ids (``officialId``, populated on
every foul since 2019-20), wall-clock timestamps (``timeActual``), and shot
coordinates (``xLegacy``/``yLegacy``, on 2pt/3pt shots only); fouls and blocks carry
a court zone (``area``/``areaDetail``) instead.

**Transport note (Task 5 spike, 2026-09-26):** plain HTTP
(:func:`sportsdataverse.dl_utils.download`) with hoopR's ``.nba_cdn_headers()``-
equivalent browser headers (Chrome UA, ``Origin``/``Referer: https://www.nba.com``)
returned a 403 HTML "Access Denied" body for both ``cdn.nba.com`` and
``cdn.wnba.com``. The refusal is header- and protocol-based, not a TLS fingerprint:
a later probe matrix (2026-09-26) got 200 JSON from plain ``requests`` carrying a
full Chrome header set, and the 403 page for a client's default headers. This module
routes through :func:`sportsdataverse.nba.nba_stats_runtime._curl_transport`
(curl_cffi Chrome impersonation, the same transport as stats.nba.com), which sends
a browser-consistent request and returned 200 JSON for both hosts. See
``tests/nba/fixtures/nba_live/README.md`` / ``tests/wnba/fixtures/wnba_live/README.md``
for the capture record.

Port of hoopR's ``nba_live_pbp`` / ``nba_live_boxscore`` (R/nba_stats_pbp.R) --
the WNBA twins in :mod:`sportsdataverse.wnba.wnba_live` share this module's parser
and fetch helper, matching wehoop's ``wnba_live_pbp`` / ``wnba_live_boxscore``.
"""

from __future__ import annotations

import json as _json
import os
import re
import time
from typing import Any, Callable, Optional

import polars as pl

from sportsdataverse.dl_utils import underscore
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.nba.nba_officiating import _as_dict, _gid, _l2m_gid, _records
from sportsdataverse.nba.nba_stats_runtime import _curl_transport

__all__ = [
    "NBA_LIVE_PBP_CORE_SCHEMA",
    "NBA_LIVE_OFFICIALS_CORE_SCHEMA",
    "NBA_LIVE_PLAYERS_CORE_SCHEMA",
    "NBA_LIVE_TEAM_CORE_SCHEMA",
    "NBA_LIVE_GAME_CORE_SCHEMA",
    "parse_nba_live_pbp",
    "parse_nba_live_boxscore",
    "nba_live_pbp",
    "nba_live_boxscore",
]

Transport = Callable[[str, dict, dict, Optional[str]], tuple]

_CDN_HOSTS = {"nba": "cdn.nba.com", "wnba": "cdn.wnba.com"}
_ID_COL_RE = re.compile(r"(^(official_id|person_id|team_id)$)|(_person_id$)")

# Statuses worth a retry -- throttle/5xx, mirroring nba_stats_runtime._get's
# retryable failure modes for the same curl_cffi transport family. 403 is
# deliberately excluded: it is always definitive here (S3 AccessDenied ->
# NoDataError, Akamai/WAF HTML -> AssetFetchError), never a transient block.
_RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
# Module-level reference (not a bare `time.sleep` call) so tests can
# monkeypatch it without touching the real `time` module.
_sleep = time.sleep

# Ruling R4 (Task 5 review, fix round 1): every returned frame carries these core
# columns with these dtypes -- even at height 0 -- so `pl.concat` across games/sides
# never breaks on a schema mismatch. Names below are fixture-verified against the
# 2025-26 NBA capture (`tests/nba/fixtures/nba_live/`); none needed correcting from
# the reviewer's proposed names.
NBA_LIVE_PBP_CORE_SCHEMA = pl.Schema(
    {
        "game_id": pl.Utf8,
        "action_number": pl.Int64,
        "period": pl.Int64,
        "clock": pl.Utf8,
        "time_actual": pl.Utf8,
        "action_type": pl.Utf8,
        "sub_type": pl.Utf8,
        "team_id": pl.Int64,
        "person_id": pl.Int64,
        "official_id": pl.Int64,
        "x_legacy": pl.Float64,
        "y_legacy": pl.Float64,
        "description": pl.Utf8,
    }
)
NBA_LIVE_OFFICIALS_CORE_SCHEMA = pl.Schema(
    {
        "game_id": pl.Utf8,
        "person_id": pl.Int64,
        "name": pl.Utf8,
        "jersey_num": pl.Utf8,
        "assignment": pl.Utf8,
    }
)
NBA_LIVE_PLAYERS_CORE_SCHEMA = pl.Schema(
    {
        "game_id": pl.Utf8,
        "team_id": pl.Int64,
        "person_id": pl.Int64,
        "name": pl.Utf8,
        "jersey_num": pl.Utf8,
        "position": pl.Utf8,
        "starter": pl.Utf8,
        "played": pl.Utf8,
    }
)
NBA_LIVE_TEAM_CORE_SCHEMA = pl.Schema(
    {
        "game_id": pl.Utf8,
        "team_id": pl.Int64,
        "team_tricode": pl.Utf8,
        "score": pl.Int64,
    }
)
NBA_LIVE_GAME_CORE_SCHEMA = pl.Schema(
    {
        "game_id": pl.Utf8,
        "game_status": pl.Int64,
        "game_time_utc": pl.Utf8,
        "home_team_id": pl.Int64,
        "away_team_id": pl.Int64,
        "attendance": pl.Int64,
    }
)


def _is_id_col(name: str) -> bool:
    """True for join-key id columns (``official_id``/``person_id``/``team_id``/``*_person_id``)."""
    return bool(_ID_COL_RE.search(name))


def _cdn_headers(league: str = "nba") -> dict[str, str]:
    """Build the browser-mimicking headers cdn.nba.com/cdn.wnba.com require.

    Without a browser request plus a matching ``Origin``/``Referer``, the CDN
    returns an "Access Denied" HTML page instead of JSON -- mirrors hoopR's
    ``.nba_cdn_headers()`` / wehoop's ``.wnba_cdn_headers()``. There is no
    ``User-Agent`` here: curl_cffi's Chrome impersonation sends one that matches
    its ``sec-ch-ua`` client hints, and an explicit one (Chrome/120 against
    impersonated Chrome 146, measured) made the two disagree.

    Args:
        league: ``"nba"`` or ``"wnba"``. Selects the ``Origin``/``Referer`` host.

    Returns:
        A dict of HTTP request headers.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_live import _cdn_headers
            h = _cdn_headers("wnba")
            print(h["Origin"])  # "https://www.wnba.com"
    """
    is_wnba = league == "wnba"
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.wnba.com" if is_wnba else "https://www.nba.com",
        "Referer": "https://www.wnba.com/" if is_wnba else "https://www.nba.com/",
    }


def _fetch_live(
    kind: str,
    game_id: str | int,
    *,
    league: str = "nba",
    proxy: dict[str, str] | None = None,
    transport: Transport | None = None,
) -> dict[str, Any]:
    """Fetch a cdn.nba.com / cdn.wnba.com liveData payload, classifying failures.

    The one shared fetch helper behind ``nba_live_pbp``/``nba_live_boxscore`` and
    the WNBA shims in :mod:`sportsdataverse.wnba.wnba_live` -- ``league`` picks the
    host and browser-header set; everything else is identical.

    Retries on a bounded budget shared with
    :func:`sportsdataverse.nba.nba_stats_runtime._get` (same curl_cffi transport
    family): ``SDV_PY_NBA_STATS_RETRIES`` (attempts beyond the first, default
    ``0`` -- byte-identical to a single shot unless a caller opts in) and
    ``SDV_PY_NBA_STATS_BACKOFF`` (seconds, multiplied by the attempt number,
    default ``1.5``). A transport exception (other than ``ImportError``), a
    throttle/5xx status ({408, 429, 500, 502, 503, 504}), or a 200 response whose
    body is not a JSON object carrying the liveData ``game`` object are all retried;
    a 404 or a 403 carrying the S3 ``<Code>AccessDenied</Code>`` marker is never
    retried -- it is a definitive "no data".

    Args:
        kind: ``"playbyplay"`` or ``"boxscore"``.
        game_id: NBA/WNBA game ID (int or str). Zero-padded to 10 digits; checked by
            :func:`sportsdataverse.nba.nba_officiating._gid` before any request.
        league: ``"nba"`` (``cdn.nba.com``) or ``"wnba"`` (``cdn.wnba.com``).
        proxy: Optional proxy dict in the ``requests`` ``proxies=`` shape
            (``{"https": "http://host:port"}``); the ``https`` entry (falling back
            to ``http``) is forwarded to the curl_cffi transport.
        transport: Injectable ``(url, params, headers, proxy_url) -> (status, text)``
            callable, defaulting to :func:`sportsdataverse.nba.nba_stats_runtime._curl_transport`.
            Tests monkeypatch this module's ``_curl_transport`` to stay offline.

    Returns:
        The parsed JSON payload (dict).

    Raises:
        ValueError: ``game_id`` is not one non-negative integer id (a bool, a
            negative or fractional number, a string that is not all digits, or more than 10 digits).
        NoDataError: 404, or a 403 whose body carries the S3
            ``<Code>AccessDenied</Code>`` marker -- no liveData object exists for
            this game (too old, or not yet started). Never retried.
        AssetFetchError: A WAF/bot-check block (403 HTML), a throttle/5xx status,
            or a 200 whose body is not a JSON object carrying the liveData ``game``
            object (an error envelope or a changed schema), once the retry budget
            above is exhausted; also raised (chained via ``from exc``)
            once a transport exception (a curl_cffi timeout, connection error, or
            TLS failure) exhausts the same budget -- it never escapes as a bare
            exception, it is always reclassified into the error vocabulary.
        ImportError: Propagates unchanged on the first attempt when curl_cffi is
            not installed -- the repo contract that a missing optional
            dependency surfaces as a clear ``ImportError`` (``pip install
            curl_cffi`` / ``sportsdataverse[all]``), never masked as
            ``AssetFetchError`` and never retried.

    Example:
        Offline with an injected transport::

            from sportsdataverse.nba.nba_live import _fetch_live
            def fake(url, params, headers, proxy_url):
                return 200, '{"game": {"actions": []}}'
            payload = _fetch_live("playbyplay", "0022500001", transport=fake)
    """
    host = _CDN_HOSTS[league]
    url = f"https://{host}/static/json/liveData/{kind}/{kind}_{_gid(game_id)}.json"
    _transport = transport or _curl_transport
    headers = _cdn_headers(league)
    proxy_url = (proxy or {}).get("https") or (proxy or {}).get("http")

    # Same env knobs as nba_stats_runtime._get (same curl_cffi transport family) --
    # reused, not reinvented. Default 0 retries keeps this a single shot, byte-
    # identical to the prior behavior, unless a caller opts in.
    retries = int(os.environ.get("SDV_PY_NBA_STATS_RETRIES", "0"))
    backoff = float(os.environ.get("SDV_PY_NBA_STATS_BACKOFF", "1.5"))

    for attempt in range(retries + 1):
        is_last = attempt == retries
        try:
            status, text = _transport(url, {}, headers, proxy_url)
        except ImportError:
            # A missing curl_cffi must surface as-is (repo contract: `pip install
            # curl_cffi` or `sportsdataverse[all]`), never retried or reclassified.
            raise
        except (NoDataError, AssetFetchError):
            # Already-classified, definitive outcomes -- never retried.
            raise
        except Exception as exc:
            if not is_last:
                _sleep(backoff * (attempt + 1))
                continue
            raise AssetFetchError(
                f"{host} liveData {kind} fetch failed (transport error) for game {_gid(game_id)}: {exc}"
            ) from exc

        if status == 404 or (status == 403 and "<Code>AccessDenied</Code>" in text[:500]):
            raise NoDataError(f"{host} has no liveData {kind} for game {_gid(game_id)}")

        payload = None
        if status == 200 and text:
            try:
                payload = _json.loads(text)
            except _json.JSONDecodeError:
                payload = None
        # Every liveData body carries a `game` object; a 200 without one is an error
        # envelope or a changed schema, never an empty game (checked for raw=True too).
        if status == 200 and isinstance(payload, dict) and isinstance(payload.get("game"), dict):
            return payload

        if status != 200 and status not in _RETRYABLE_STATUSES:
            raise AssetFetchError(f"{host} liveData {kind} fetch failed (status={status}) for game {_gid(game_id)}")

        # Retryable: a throttle/5xx status, or a 200 without the liveData `game`
        # object (H2) -- both a transient failure mode, never silently treated as "no
        # data" the way an unguarded `dict(json.loads(blank_or_bad_text))` would.
        if not is_last:
            _sleep(backoff * (attempt + 1))
            continue
        if status == 200:
            raise AssetFetchError(
                f"{host} liveData {kind} returned a 200 whose body is not a liveData JSON "
                f"object (no `game` object) for game {_gid(game_id)} after exhausting the retry budget"
            )
        raise AssetFetchError(
            f"{host} liveData {kind} fetch failed (status={status}) for game {_gid(game_id)} "
            "after exhausting the retry budget"
        )

    # Unreachable under normal env values (retries >= 0 guarantees at least one
    # iteration); kept as a defensive fallback + satisfies mypy's return-path check.
    raise AssetFetchError(f"{host} liveData {kind} fetch failed for game {_gid(game_id)}")


def _stringify(v: Any) -> Any:
    """Recursively turn every non-dict leaf into a string (lists as JSON), keeping ``None``."""
    if isinstance(v, dict):
        return {k: _stringify(x) for k, x in v.items()}
    if v is None:
        return None
    return _json.dumps(v) if isinstance(v, list) else str(v)


def _flatten(records: list[dict[str, Any]]) -> pl.DataFrame:
    """One ``pl.json_normalize`` + snake_case rename + id-column Int64 cast pass."""
    # infer_schema_length=None: default 100-row inference silently drops fields that
    # first appear later (e.g. block_person_id first seen ~action 110 on real captures).
    df = pl.json_normalize(records, separator="_", infer_schema_length=None)
    names: dict[str, str] = {}
    for c in df.columns:
        names.setdefault(underscore(c), c)
    if len(names) < df.width:
        # Two keys snake-case to one name (personId and person_id): keep the first. Only
        # then -- select/drop would zero the height of a 1-row, 0-column frame.
        df = df.select(list(names.values()))
    df = df.rename({c: n for n, c in names.items()})
    id_cols = [c for c in df.columns if _is_id_col(c)]
    return df.with_columns([_int_id(c, df.schema[c]) for c in id_cols]) if id_cols else df


def _int_id(name: str, dtype: pl.DataType) -> pl.Expr:
    """Cast an id column to Int64; a fractional float is null, never truncated into another id."""
    col = pl.col(name)
    if dtype.is_float():
        return pl.when(col == col.floor()).then(col.cast(pl.Int64, strict=False)).otherwise(None).alias(name)
    return col.cast(pl.Int64, strict=False)


def _normalize(records: list[dict[str, Any]]) -> pl.DataFrame:
    """``pl.json_normalize`` + snake_case rename + id-column Int64 cast, or an empty frame."""
    if not records:
        return pl.DataFrame()
    try:
        return _flatten(records)
    except (TypeError, ValueError, pl.exceptions.PolarsError):
        # A cell of an unexpected type (a list mixing ints and strings, a list or object
        # id, one key typed two ways): flatten stringified leaves rather than raise, per
        # the parser contract, so an id that cannot cast is null. Real captures never
        # take this path, so their dtypes are unchanged.
        return _flatten([_stringify(r) for r in records])


def _ensure_core_schema(df: pl.DataFrame, schema: pl.Schema) -> pl.DataFrame:
    """Guarantee every column of *schema* is present with the right dtype (Ruling R4).

    A completely empty frame (``height == 0``, the shape :func:`_normalize` returns
    for an empty record list) is replaced outright by a zero-row frame carrying
    exactly *schema* -- adding a literal column to a 0x0 frame with ``with_columns``
    would otherwise silently manufacture a phantom 1-row frame. A non-empty frame
    keeps every column it already has (including ones outside *schema*); any
    *schema* column it's missing is added as a typed null, and any it already has
    is cast to the schema's dtype so two frames built from different payloads
    (e.g. one side of a boxscore with 0 players, the other with 12) always share
    a common, ``pl.concat``-safe core schema. A list or struct where a scalar
    belongs cannot cast, so it is replaced by a typed null too.

    Args:
        df: The frame to backfill (from :func:`_normalize` or a downstream helper).
        schema: The core schema (one of the ``*_CORE_SCHEMA`` module constants).

    Returns:
        *df* with every *schema* column present at the declared dtype.

    Example:
        Backfill a zero-player boxscore side::

            from sportsdataverse.nba.nba_live import NBA_LIVE_PLAYERS_CORE_SCHEMA, _ensure_core_schema
            import polars as pl
            df = _ensure_core_schema(pl.DataFrame(), NBA_LIVE_PLAYERS_CORE_SCHEMA)
            print(df.height, df.columns)  # 0 ['game_id', 'team_id', ...]
    """
    if df.height == 0:
        return pl.DataFrame(schema=schema)
    # Int64 columns go through _int_id: a plain cast truncates a fractional float (4.5 -> 4).
    exprs = [
        (_int_id(name, df.schema[name]) if dtype == pl.Int64 else pl.col(name).cast(dtype, strict=False))
        if name in df.columns and not df.schema[name].is_nested()
        else pl.lit(None, dtype=dtype).alias(name)
        for name, dtype in schema.items()
    ]
    return df.with_columns(exprs)


def parse_nba_live_pbp(payload: dict[str, Any], *, return_as_pandas: bool = False) -> Any:
    """Parse a cdn.nba.com/cdn.wnba.com liveData play-by-play payload into a tidy frame.

    One row per ``game.actions[]`` entry. Columns are flattened
    (``pl.json_normalize(..., separator="_")``) and snake-cased via
    :func:`sportsdataverse.dl_utils.underscore`; ``official_id``, ``person_id``,
    ``team_id``, and any ``*_person_id`` column (``assist_person_id``,
    ``jump_ball_won_person_id``, etc.) are cast to ``Int64``. A ``game_id`` column
    (the payload's own ``game.gameId``, zero-padded) is added to every row.

    Args:
        payload: The JSON payload (dict) from a cdn.nba.com/cdn.wnba.com
            ``liveData/playbyplay`` endpoint.
        return_as_pandas: If True, return a pandas DataFrame instead of polars.

    Returns:
        A DataFrame with one row per action. Every frame, including the zero-row
        frame an empty/malformed payload (missing ``game`` or ``actions``) returns,
        carries :data:`NBA_LIVE_PBP_CORE_SCHEMA`'s columns at their declared dtypes,
        so a caller can ``pl.concat`` across games without a schema mismatch. Every
        other liveData action field (``area``, ``shot_distance``,
        ``assist_person_id``, ...) is passed through when the payload carries it, so
        an event-specific column such as ``block_person_id`` is present only when the
        game had that event.

    Raises:
        This function does not raise. Empty or malformed payloads produce a
        zero-row DataFrame carrying :data:`NBA_LIVE_PBP_CORE_SCHEMA`; a cell of the
        wrong type (a list or object where a scalar belongs, a non-numeric id)
        becomes null, or its JSON text in a string column.

    Example:
        Parse a real capture::

            import json
            from sportsdataverse.nba.nba_live import parse_nba_live_pbp
            with open("playbyplay_0022500001.json") as f:
                payload = json.load(f)
            pbp = parse_nba_live_pbp(payload)
            fouls = pbp.filter(pbp["action_type"] == "foul")
            print(fouls["official_id"].null_count())  # 0 (2019-20+ contract)

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization
            * `wehoop`_ -- R package for WNBA data access and visualization

            .. _hoopR: https://hoopR.sportsdataverse.org
            .. _wehoop: https://wehoop.sportsdataverse.org
    """
    game = _as_dict(_as_dict(payload).get("game"))
    df = _normalize(_records(game.get("actions")))
    gid = _l2m_gid(game.get("gameId") or None)
    if df.height and gid:
        df = df.with_columns(pl.lit(gid).alias("game_id"))
    df = _ensure_core_schema(df, NBA_LIVE_PBP_CORE_SCHEMA)
    return df.to_pandas() if return_as_pandas else df


def parse_nba_live_boxscore(payload: dict[str, Any], *, return_as_pandas: bool = False) -> dict[str, Any]:
    """Parse a cdn.nba.com/cdn.wnba.com liveData boxscore payload into tidy frames.

    Splits the payload into six tables: game metadata, officials (crew + jersey
    numbers + ``assignment`` slot label), and one team-level + one player-level
    table per side. Columns are flattened and snake-cased the same way as
    :func:`parse_nba_live_pbp`; a ``game_id`` column is added to every table and
    ``team_id`` is added to the player tables from the enclosing team object.

    Args:
        payload: The JSON payload (dict) from a cdn.nba.com/cdn.wnba.com
            ``liveData/boxscore`` endpoint.
        return_as_pandas: If True, return pandas DataFrames instead of polars.

    Returns:
        A dict with six keys: ``"game"``, ``"officials"``, ``"home_players"``,
        ``"away_players"``, ``"home_team"``, ``"away_team"``. Missing sections
        (e.g. a game with no ``homeTeam``) yield zero-row frames for that key,
        each still carrying its ``*_CORE_SCHEMA`` columns at their declared
        dtypes -- so e.g. a boxscore where one side has 0 players still lets
        ``pl.concat([home_players, away_players], how="diagonal_relaxed")``
        succeed. Every other liveData field is passed through, snake-cased, with
        each nested ``statistics`` object flattened into ``statistics_*`` columns
        (``statistics_points``, ``statistics_plus_minus_points``, ...); a field only
        some players carry, such as ``not_playing_reason``, is present only when a
        player on that side has it. The payload's ``arena`` object and each team's
        ``periods`` list are dropped.

    Raises:
        This function does not raise. Empty or malformed payloads produce
        zero-row, core-schema DataFrames for every key; a cell of the wrong type (a
        list or object where a scalar belongs, a non-numeric id) becomes null, or its
        JSON text in a string column.

    Example:
        Parse a real capture::

            import json
            from sportsdataverse.nba.nba_live import parse_nba_live_boxscore
            with open("boxscore_0022500001.json") as f:
                payload = json.load(f)
            result = parse_nba_live_boxscore(payload)
            officials = result["officials"]
            print(officials.select("person_id", "assignment"))

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization
            * `wehoop`_ -- R package for WNBA data access and visualization

            .. _hoopR: https://hoopR.sportsdataverse.org
            .. _wehoop: https://wehoop.sportsdataverse.org
    """
    g = _as_dict(_as_dict(payload).get("game"))
    gid = _l2m_gid(g.get("gameId") or None)

    def _with_gid(df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(pl.lit(gid).alias("game_id")) if df.height and gid else df

    def _team_frame(team: dict[str, Any]) -> pl.DataFrame:
        flat = {k: v for k, v in team.items() if k not in ("players", "periods")}
        df = _with_gid(_normalize([flat] if flat else []))
        return _ensure_core_schema(df, NBA_LIVE_TEAM_CORE_SCHEMA)

    def _team_id(v: Any) -> pl.Expr:
        # A list/object/bool teamId, or a fractional number, is null: never an error
        # and never a truncated id.
        if isinstance(v, (dict, list, bool)) or (isinstance(v, float) and not v.is_integer()):
            v = None
        return pl.lit(v).cast(pl.Int64, strict=False)

    def _players_frame(team: dict[str, Any]) -> pl.DataFrame:
        df = _normalize(_records(team.get("players")))
        if df.height:
            df = df.with_columns(_team_id(team.get("teamId")).alias("team_id"))
        df = _with_gid(df)
        return _ensure_core_schema(df, NBA_LIVE_PLAYERS_CORE_SCHEMA)

    game_meta = {k: v for k, v in g.items() if k not in ("officials", "homeTeam", "awayTeam", "arena")}
    home_team = _as_dict(g.get("homeTeam"))
    away_team = _as_dict(g.get("awayTeam"))

    game_df = _with_gid(_normalize([game_meta] if game_meta else []))
    if game_df.height:
        game_df = game_df.with_columns(
            _team_id(home_team.get("teamId")).alias("home_team_id"),
            _team_id(away_team.get("teamId")).alias("away_team_id"),
        )

    out = {
        "game": _ensure_core_schema(game_df, NBA_LIVE_GAME_CORE_SCHEMA),
        "officials": _ensure_core_schema(
            _with_gid(_normalize(_records(g.get("officials")))), NBA_LIVE_OFFICIALS_CORE_SCHEMA
        ),
        "home_players": _players_frame(home_team),
        "away_players": _players_frame(away_team),
        "home_team": _team_frame(home_team),
        "away_team": _team_frame(away_team),
    }
    return {k: (v.to_pandas() if return_as_pandas else v) for k, v in out.items()}


def nba_live_pbp(
    game_id: str | int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    proxy: dict[str, str] | None = None,
) -> Any:
    """Fetch and parse NBA cdn.nba.com liveData play-by-play for a game.

    Retrieves ``https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json``
    and parses it via :func:`parse_nba_live_pbp`. Unlike stats.nba.com's
    play-by-play, this feed carries per-whistle referee ids (``official_id``,
    populated on every foul since the 2019-20 season) and wall-clock timestamps
    (``time_actual``).

    Args:
        game_id: NBA game ID (int or str). Zero-padded to 10 digits.
        raw: If True, return the raw JSON payload (dict) instead of a DataFrame.
        return_as_pandas: If True, return a pandas DataFrame instead of polars.
        proxy: Optional proxy dict passed through to the HTTP layer.

    Returns:
        If ``raw=True``, the raw JSON dict. Otherwise, a DataFrame with one row per
        action, parsed by :func:`parse_nba_live_pbp`. Its 13 core columns
        (``game_id``, ``action_number``, ``period``, ``clock``, ``time_actual``,
        ``action_type``, ``sub_type``, ``team_id``, ``person_id``, ``official_id``,
        ``x_legacy``, ``y_legacy``, ``description``) are guaranteed on every frame,
        even a zero-row one, at their declared dtypes. Every other liveData action
        field is passed through, snake-cased, when the payload carries it, so an
        event-specific column such as ``block_person_id`` or
        ``foul_drawn_person_id`` is present only when the game had that event.

    Raises:
        ValueError: ``game_id`` is not one non-negative integer id -- a bool, a
            negative or fractional number, a string that is not all digits, or more than 10 digits
            (checked before any request).
        NoDataError: The game has no liveData play-by-play object (too old, or
            not yet started).
        AssetFetchError: The fetch failed (network error, rate limit, or a
            bot-check block), or the 200 body is not a JSON object carrying the
            liveData ``game`` object -- checked with ``raw=True`` too.
        ImportError: curl_cffi is not installed -- required for the live
            transport (``pip install curl_cffi`` / ``sportsdataverse[all]``).

    Example:
        Fetch a game's live play-by-play::

            from sportsdataverse.nba.nba_live import nba_live_pbp
            pbp = nba_live_pbp("0022500001")
            print(pbp.filter(pbp["action_type"] == "foul").height)

        Pipeline next step (fouls with a referee id)::

            fouls = pbp.filter(pbp["action_type"] == "foul").select("official_id", "time_actual")

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization
            * `wehoop`_ -- R package for WNBA data access and visualization

            .. _hoopR: https://hoopR.sportsdataverse.org
            .. _wehoop: https://wehoop.sportsdataverse.org
    """
    payload = _fetch_live("playbyplay", game_id, league="nba", proxy=proxy)
    return payload if raw else parse_nba_live_pbp(payload, return_as_pandas=return_as_pandas)


def nba_live_boxscore(
    game_id: str | int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    proxy: dict[str, str] | None = None,
) -> Any:
    """Fetch and parse NBA cdn.nba.com liveData boxscore for a game.

    Retrieves ``https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json``
    and parses it via :func:`parse_nba_live_boxscore` into six tables (game,
    officials, home/away players, home/away team).

    Args:
        game_id: NBA game ID (int or str). Zero-padded to 10 digits.
        raw: If True, return the raw JSON payload (dict) instead of parsed DataFrames.
        return_as_pandas: If True, return pandas DataFrames instead of polars.
        proxy: Optional proxy dict passed through to the HTTP layer.

    Returns:
        If ``raw=True``, the raw JSON dict. Otherwise, a dict of six DataFrames
        (``game``, ``officials``, ``home_players``, ``away_players``,
        ``home_team``, ``away_team``) parsed by :func:`parse_nba_live_boxscore`.
        Their core columns are guaranteed on every frame, even a zero-row one, at
        their declared dtypes: ``game_id`` on all six, plus ``game_status``,
        ``game_time_utc``, ``home_team_id``, ``away_team_id``, and ``attendance`` on
        ``game``; ``person_id``, ``name``, ``jersey_num``, and ``assignment`` on
        ``officials``; ``team_id``, ``person_id``, ``name``, ``jersey_num``,
        ``position``, ``starter``, and ``played`` on the player frames; and
        ``team_id``, ``team_tricode``, and ``score`` on the team frames. Every other
        liveData field is passed through, snake-cased, with each nested
        ``statistics`` object flattened into ``statistics_*`` columns; a field only
        some players carry, such as ``not_playing_reason``, is present only when a
        player on that side has it.

    Raises:
        ValueError: ``game_id`` is not one non-negative integer id -- a bool, a
            negative or fractional number, a string that is not all digits, or more than 10 digits
            (checked before any request).
        NoDataError: The game has no liveData boxscore object (too old, or not
            yet started).
        AssetFetchError: The fetch failed (network error, rate limit, or a
            bot-check block), or the 200 body is not a JSON object carrying the
            liveData ``game`` object -- checked with ``raw=True`` too.
        ImportError: curl_cffi is not installed -- required for the live
            transport (``pip install curl_cffi`` / ``sportsdataverse[all]``).

    Example:
        Fetch a game's live boxscore::

            from sportsdataverse.nba.nba_live import nba_live_boxscore
            result = nba_live_boxscore("0022500001")
            officials = result["officials"]
            print(officials.select("person_id", "name", "assignment"))

        See Also:
            * `hoopR`_ -- R package for NBA data access and visualization
            * `wehoop`_ -- R package for WNBA data access and visualization

            .. _hoopR: https://hoopR.sportsdataverse.org
            .. _wehoop: https://wehoop.sportsdataverse.org
    """
    payload = _fetch_live("boxscore", game_id, league="nba", proxy=proxy)
    return payload if raw else parse_nba_live_boxscore(payload, return_as_pandas=return_as_pandas)
