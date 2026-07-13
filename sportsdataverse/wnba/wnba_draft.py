"""ESPN WNBA draft picks scraper.

Single ESPN endpoint:
    site.web.api.espn.com/apis/site/v2/sports/basketball/wnba/draft?season={year}

ESPN ships the modern draft response with each pick inlined under
``picks[]``, carrying the rich athlete metadata (display name, height,
position id, college team, headshot, ESPN profile link) the older
``sports.core.api.espn.com`` ``/draft/rounds`` endpoint required a separate
``$ref`` resolution to fetch. This wrapper flattens that ``picks[]`` array
to a single polars DataFrame, one row per pick.

Fields ESPN does not inline on the draft response (e.g. ``firstName`` /
``lastName``, ``weight``, ``age``, birth city / state, full position name,
school id) come back as ``None``; resolve them via
``espn_wnba_athlete_info`` (or the matching wehoop R wrapper) using the
returned ``athlete_id``.
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download

_OUTPUT_COLUMNS: list[str] = [
    "season",
    "round_number",
    "pick_number",
    "overall_pick",
    "team_id",
    "team_abbreviation",
    "team_display_name",
    "athlete_id",
    "athlete_first_name",
    "athlete_last_name",
    "athlete_full_name",
    "athlete_display_name",
    "athlete_position_id",
    "athlete_position_name",
    "athlete_position_abbreviation",
    "athlete_height",
    "athlete_weight",
    "athlete_age",
    "athlete_birth_city",
    "athlete_birth_state",
    "headshot_href",
    "school_id",
    "school_name",
    "school_abbreviation",
    "link_web",
]


@overload
def espn_wnba_draft(
    season: int,
    *,
    raw: Literal[True],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> dict[str, Any]: ...
@overload
def espn_wnba_draft(
    season: int,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> pd.DataFrame: ...
@overload
def espn_wnba_draft(
    season: int,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def espn_wnba_draft(
    season: int,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull ESPN WNBA draft picks for a season.

    Args:
        season: Season year (e.g. ``2024`` for the 2024 WNBA Draft).
            Forwarded to ESPN as ``?season=YYYY``.
        raw: If True, returns the parsed JSON dict before any flattening.
        return_as_pandas: If True, returns a pandas DataFrame; otherwise
            polars.
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        Polars (or pandas) DataFrame with one row per draft pick.
        Documented columns: ``season``, ``round_number``, ``pick_number``,
        ``overall_pick``, ``team_id``, ``team_abbreviation``,
        ``team_display_name``, ``athlete_id``, ``athlete_first_name``,
        ``athlete_last_name``, ``athlete_full_name``,
        ``athlete_display_name``, ``athlete_position_id``,
        ``athlete_position_name``, ``athlete_position_abbreviation``,
        ``athlete_height``, ``athlete_weight``, ``athlete_age``,
        ``athlete_birth_city``, ``athlete_birth_state``, ``headshot_href``,
        ``school_id``, ``school_name``, ``school_abbreviation``,
        ``link_web``.

        Fields ESPN does not inline on the draft response (e.g.
        first / last name, weight, age, birth location, school id) come
        back as ``None``; resolve them via the athlete-info endpoint
        using the returned ``athlete_id``.

        If ``raw=True``, returns the raw response dict.

    Raises:
        sportsdataverse.errors.NoESPNDataError: ESPN returned 404.
        requests.exceptions.RequestException: Other network failures after
            retries.

    Example:
        Pull a single draft year — one row per pick::

            from sportsdataverse.wnba import espn_wnba_draft
            draft = espn_wnba_draft(season=2024)
            print(draft.shape)
            draft.select(
                ["overall_pick", "round_number", "team_abbreviation", "athlete_display_name", "school_name"]
            ).head(12)

        First-round picks only::

            import polars as pl
            draft.filter(pl.col("round_number") == 1).head()

        Pandas round-trip — convenient for joining against your own roster table::

            draft_pd = espn_wnba_draft(season=2024, return_as_pandas=True)
            draft_pd[["overall_pick", "athlete_display_name", "school_name"]].head()

        See Also:
            * `wehoop`_ — R sister package; mirrors this surface
            * `nba_api`_ — alternative Python source for NBA/WNBA stats endpoints
            * `hoopR`_ — companion R package for men's basketball

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    url = f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/wnba/draft?season={season}"
    resp = download(url, **kwargs)
    payload: dict[str, Any] = resp.json()

    if raw:
        return payload

    rows = list(_iter_pick_rows(payload, season))

    if not rows:
        empty = pl.DataFrame(schema=_build_schema())
        return empty.to_pandas() if return_as_pandas else empty

    df = pl.DataFrame(rows, schema=_build_schema())
    return df.to_pandas() if return_as_pandas else df


def _build_schema() -> dict[str, type[pl.DataType] | pl.DataType]:
    int_cols = {
        "season",
        "round_number",
        "pick_number",
        "overall_pick",
        "team_id",
        "athlete_id",
        "athlete_position_id",
        "athlete_age",
        "school_id",
    }
    float_cols = {"athlete_weight"}
    schema: dict[str, type[pl.DataType] | pl.DataType] = {}
    for col in _OUTPUT_COLUMNS:
        if col in int_cols:
            schema[col] = pl.Int64
        elif col in float_cols:
            schema[col] = pl.Float64
        else:
            schema[col] = pl.Utf8
    return schema


def _iter_pick_rows(payload: dict[str, Any], season: int) -> Any:
    """Yield one row dict per inlined pick under ``payload['picks']``.

    Falls back to walking ``rounds[].picks[]`` if ESPN reverts to the
    nested-rounds shape.
    """
    picks_raw = payload.get("picks")
    if isinstance(picks_raw, list) and picks_raw:
        for pick in picks_raw:
            row = _pick_to_row(pick, season=season)
            if row is not None:
                yield row
        return

    rounds = payload.get("rounds")
    if isinstance(rounds, list):
        for rd in rounds:
            if not isinstance(rd, dict):
                continue
            round_no = _coerce_int(rd.get("number"))
            for pick in rd.get("picks") or []:
                row = _pick_to_row(pick, season=season, round_default=round_no)
                if row is not None:
                    yield row


def _pick_to_row(
    pick: Any,
    season: int,
    round_default: int | None = None,
) -> dict[str, Any] | None:
    """Flatten a single inlined ESPN pick dict to an output row."""
    if not isinstance(pick, dict):
        return None

    athlete = pick.get("athlete") or {}
    if not isinstance(athlete, dict):
        athlete = {}
    position = athlete.get("position") or {}
    if not isinstance(position, dict):
        position = {}
    school = athlete.get("team") or {}
    if not isinstance(school, dict):
        school = {}
    headshot = athlete.get("headshot") or {}
    if not isinstance(headshot, dict):
        headshot = {}

    row: dict[str, Any] = {col: None for col in _OUTPUT_COLUMNS}
    row["season"] = int(season)
    row["round_number"] = _coerce_int(pick.get("round")) or round_default
    row["pick_number"] = _coerce_int(pick.get("pick"))
    row["overall_pick"] = _coerce_int(pick.get("overall"))

    # Team id ships at the pick level on the modern endpoint; ``team`` may
    # also exist as a richer dict on some payloads.
    pick_team = pick.get("team") if isinstance(pick.get("team"), dict) else {}
    row["team_id"] = _coerce_int(pick.get("teamId") or pick_team.get("id"))
    row["team_abbreviation"] = _stringify(pick_team.get("abbreviation"))
    row["team_display_name"] = _stringify(pick_team.get("displayName"))

    row["athlete_id"] = _coerce_int(athlete.get("id") or athlete.get("alternativeId"))
    row["athlete_first_name"] = _stringify(athlete.get("firstName"))
    row["athlete_last_name"] = _stringify(athlete.get("lastName"))
    row["athlete_full_name"] = _stringify(athlete.get("fullName"))
    row["athlete_display_name"] = _stringify(athlete.get("displayName"))
    row["athlete_position_id"] = _coerce_int(position.get("id"))
    row["athlete_position_name"] = _stringify(position.get("displayName") or position.get("name"))
    row["athlete_position_abbreviation"] = _stringify(position.get("abbreviation"))
    row["athlete_height"] = _stringify(athlete.get("displayHeight") or athlete.get("height"))
    row["athlete_weight"] = _coerce_float(athlete.get("weight"))
    row["athlete_age"] = _coerce_int(athlete.get("age"))

    birth = athlete.get("birthPlace") if isinstance(athlete.get("birthPlace"), dict) else {}
    row["athlete_birth_city"] = _stringify(birth.get("city"))
    row["athlete_birth_state"] = _stringify(birth.get("state"))

    row["headshot_href"] = _stringify(headshot.get("href"))

    row["school_id"] = _coerce_int(school.get("id"))
    row["school_name"] = _stringify(school.get("displayName") or school.get("name") or school.get("location"))
    row["school_abbreviation"] = _stringify(school.get("abbreviation"))

    row["link_web"] = _stringify(athlete.get("link") or athlete.get("proLink"))

    return row


def _coerce_int(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            try:
                return int(float(s))
            except ValueError:
                return None
    return None


def _coerce_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _stringify(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, str):
        return v
    return str(v)


# --- wehoop-wnba-data release producer --------------------------------------
# Port of the script-local ``parse_one_pick`` in
# ``wehoop-wnba-data/R/espn_wnba_08_draft_creation.R``. Distinct lineage from
# the live ``espn_wnba_draft`` wrapper above: this consumes the stored
# ``wnba/draft/json/{year}.json`` payload and matches the released
# ``espn_wnba_draft`` parquet (the parity oracle).

_DRAFT_INT32_COLS: tuple[str, ...] = (
    "season",
    "round",
    "pick",
    "overall_pick",
    "athlete_id",
    "college_id",
    "team_id",
)

# parse_one_pick's tibble column order (35 columns).
_DRAFT_COLUMNS: tuple[str, ...] = (
    "season",
    "round",
    "round_display_name",
    "pick",
    "overall_pick",
    "pick_traded",
    "pick_notes",
    "athlete_id",
    "athlete_uid",
    "athlete_guid",
    "athlete_first_name",
    "athlete_last_name",
    "athlete_full_name",
    "athlete_display_name",
    "athlete_short_name",
    "athlete_height",
    "athlete_weight",
    "athlete_position_abbreviation",
    "athlete_position_name",
    "athlete_headshot_href",
    "college_id",
    "college_name",
    "college_short_name",
    "college_abbreviation",
    "team_id",
    "team_uid",
    "team_slug",
    "team_location",
    "team_name",
    "team_abbreviation",
    "team_display_name",
    "team_short_display_name",
    "team_color",
    "team_alternate_color",
    "team_logo",
)


def _safe_chr(x: Any) -> str | None:
    """R ``safe_chr``: NULL/empty -> NA; else as.character of the first element.

    Delegates to the one shared emulation (``_rel_chr``) rather than keeping a
    second copy: the two had drifted apart, each missing the other's fix (this
    one mishandled non-integer floats, that one mishandled bools).
    """
    from sportsdataverse.wbb.wbb_game_rosters import _rel_chr

    return _rel_chr(x)


def _first_non_blank(*vals: str | None) -> str | None:
    """R ``%|%`` chain: skip NULL/NA/empty-string values, else NA."""
    for v in vals:
        if v is not None and v != "":
            return v
    return None


def _safe_int(s: str | None) -> int | None:
    """R ``suppressWarnings(as.integer(<chr>))``: truncating parse, NA on failure."""
    if s is None:
        return None
    try:
        return int(s)
    except ValueError:
        try:
            return int(float(s))
        except ValueError:
            return None


def _parse_one_pick(season: int, round_meta: dict[str, str | None], pk: dict[str, Any]) -> dict[str, Any]:
    """One pick's 35-column row in the R ``parse_one_pick`` order."""
    athlete: dict[str, Any] = pk.get("athlete") or {}
    team: dict[str, Any] = pk.get("team") or {}
    college: dict[str, Any] = athlete.get("college") or pk.get("college") or {}
    position: dict[str, Any] = athlete.get("position") or {}
    logos = team.get("logos") or []
    # R: team[["logo"]] %||% purrr::pluck(team, "logos", 1, "href") -- NULL-coalesce.
    logo = team.get("logo")
    if logo is None and logos:
        logo = (logos[0] or {}).get("href")
    return {
        "season": season,
        "round": _safe_int(_first_non_blank(_safe_chr(pk.get("round")), round_meta["round_number"])),
        "round_display_name": round_meta["round_display_name"],
        "pick": _safe_int(_safe_chr(pk.get("pick"))),
        "overall_pick": _safe_int(_first_non_blank(_safe_chr(pk.get("overall")), _safe_chr(pk.get("overallPick")))),
        "pick_traded": _safe_chr(pk.get("traded")),
        "pick_notes": _first_non_blank(_safe_chr(pk.get("notes")), _safe_chr(pk.get("note"))),
        "athlete_id": _safe_int(_safe_chr(athlete.get("id"))),
        "athlete_uid": _safe_chr(athlete.get("uid")),
        "athlete_guid": _safe_chr(athlete.get("guid")),
        "athlete_first_name": _safe_chr(athlete.get("firstName")),
        "athlete_last_name": _safe_chr(athlete.get("lastName")),
        "athlete_full_name": _first_non_blank(
            _safe_chr(athlete.get("fullName")), _safe_chr(athlete.get("displayName"))
        ),
        "athlete_display_name": _safe_chr(athlete.get("displayName")),
        "athlete_short_name": _safe_chr(athlete.get("shortName")),
        "athlete_height": _first_non_blank(_safe_chr(athlete.get("displayHeight")), _safe_chr(athlete.get("height"))),
        "athlete_weight": _first_non_blank(_safe_chr(athlete.get("displayWeight")), _safe_chr(athlete.get("weight"))),
        "athlete_position_abbreviation": _safe_chr(position.get("abbreviation")),
        "athlete_position_name": _safe_chr(position.get("displayName")),
        "athlete_headshot_href": _safe_chr((athlete.get("headshot") or {}).get("href")),
        "college_id": _safe_int(_safe_chr(college.get("id"))),
        "college_name": _first_non_blank(_safe_chr(college.get("name")), _safe_chr(college.get("displayName"))),
        "college_short_name": _safe_chr(college.get("shortName")),
        "college_abbreviation": _safe_chr(college.get("abbreviation")),
        "team_id": _safe_int(_first_non_blank(_safe_chr(team.get("id")), _safe_chr(pk.get("teamId")))),
        "team_uid": _safe_chr(team.get("uid")),
        "team_slug": _safe_chr(team.get("slug")),
        "team_location": _safe_chr(team.get("location")),
        "team_name": _safe_chr(team.get("name")),
        "team_abbreviation": _safe_chr(team.get("abbreviation")),
        "team_display_name": _safe_chr(team.get("displayName")),
        "team_short_display_name": _safe_chr(team.get("shortDisplayName")),
        "team_color": _safe_chr(team.get("color")),
        "team_alternate_color": _safe_chr(team.get("alternateColor")),
        "team_logo": _safe_chr(logo),
    }


def helper_wnba_draft(payload: dict, *, season: int) -> pl.DataFrame:
    """Parse one season's stored draft JSON into the released draft frame.

    Faithful polars port of the script-local ``parse_one_pick`` /
    ``build_season_draft`` parsers in
    ``wehoop-wnba-data/R/espn_wnba_08_draft_creation.R``. Handles both payload
    shapes: ``rounds[]`` of round objects each carrying ``picks[]``, and the
    modern flat top-level ``picks[]`` (where ``rounds`` is an integer count --
    the R ``is.list()`` guard skips it and ``round`` comes from each pick).
    Column set, order, and dtypes match the R-released ``espn_wnba_draft``
    parquet: Int32 ids/ordinals, String everything else, ``pick_traded`` as
    ``"TRUE"``/``"FALSE"`` (R ``as.character`` on a logical).

    Args:
        payload: The season's ``wnba/draft/json/{year}.json`` as a dict.
        season: Draft year the payload belongs to.

    Returns:
        pl.DataFrame: One row per pick, deduped and sorted by
        ``overall_pick``, ``round``, ``pick``. Empty (zero-column) frame when
        no picks parse -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba import helper_wnba_draft
            payload = json.load(open("2026.json", encoding="utf-8"))
            df = helper_wnba_draft(payload, season=2026)
            print(df.shape)

        Pipeline next step (one line)::

            df.select("overall_pick", "athlete_display_name", "team_id").head()

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    pieces: list[dict[str, Any]] = []
    rounds = payload.get("rounds")
    # ESPN's modern payloads set `rounds` to an integer count; only iterate an
    # actual array of round objects (R is.list guard).
    if isinstance(rounds, list):
        for r in rounds:
            if not isinstance(r, dict):
                continue
            rmeta: dict[str, str | None] = {
                "round_number": _first_non_blank(_safe_chr(r.get("number")), _safe_chr(r.get("round"))),
                "round_display_name": _first_non_blank(_safe_chr(r.get("displayName")), _safe_chr(r.get("name"))),
            }
            for p in r.get("picks") or []:
                pieces.append(_parse_one_pick(season, rmeta, p))
    flat_meta: dict[str, str | None] = {"round_number": None, "round_display_name": None}
    for p in payload.get("picks") or []:
        pieces.append(_parse_one_pick(season, flat_meta, p))
    if not pieces:
        return pl.DataFrame()
    df = pl.DataFrame(
        {c: [row[c] for row in pieces] for c in _DRAFT_COLUMNS},
        schema={c: (pl.Int32 if c in _DRAFT_INT32_COLS else pl.Utf8) for c in _DRAFT_COLUMNS},
        strict=False,
    )
    # R: distinct() then arrange(across(any_of(c("overall_pick", "round", "pick")))).
    return df.unique(maintain_order=True, keep="first").sort(
        ["overall_pick", "round", "pick"], nulls_last=True, maintain_order=True
    )
