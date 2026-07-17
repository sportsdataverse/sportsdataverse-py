"""ESPN athlete core records -- identity + bio season-builder release producer.

Why this producer exists: the ``player_season_stats`` payload carries NO athlete
identity whatsoever -- no name, no bio, and not even the athlete id (its only
carrier is the *filename*). Its ``"height"`` keys are team-logo pixel heights and
its ``"fullName"`` keys are arena names. Identity is therefore joined in from
``player_box`` (see :func:`build_nba_player_identity_lookup`), and bio had no
source at all until the ``{lg}/player_core/json/{athlete_id}.json`` raw dataset.

This is the producer for that dataset. The raw source is ESPN's core-v2
``/athletes/{id}`` resource, which resolves for ~100% of athletes in every era
sampled -- unlike the season-stats endpoint, which 404s constantly.

Cross-league: the core-v2 athlete resource is the SAME payload shape for
nba/wnba/mbb/wbb (30 keys common to all four; the deltas are pro-only
``contract``/``seasons`` and college-only ``proAthlete``/``flag``, all optional
here). So this implements once and the sibling league modules re-export it --
the same pattern as :func:`sportsdataverse.nba.helper_nba_officials`.

Two traps this producer encodes, both permanent properties of the source:

* **``team_id`` is the athlete's CURRENT team, not their team in any past
  season.** The payload's ``team.$ref`` is literally
  ``/seasons/{current_season}/teams/{id}``. Season team belongs to
  ``player_season_stats.statistics[].teamId`` or to ``player_box`` -- never to
  this frame. The column is named ``current_team_id`` so a join can't quietly
  pretend otherwise.
* **Bio is a Type-1 (overwriting) snapshot.** ESPN serves today's height /
  weight / jersey, not the value during season Y. Era-correct bio is not
  obtainable from this or any other ESPN endpoint. A season-partitioned
  ``player_core_{season}.parquet`` therefore means "the athletes who appeared in
  season Y, with their CURRENT bio" -- the season dimension is participation,
  not the bio's vintage.

``college`` and ``team`` arrive as ``{"$ref": url}`` only. Hydrating them would
triple the request count across the whole athlete universe, so the ids are
parsed straight out of the ref URL instead -- no extra HTTP.
"""

from __future__ import annotations

import re
from typing import Any

import polars as pl

from sportsdataverse.wbb.wbb_game_rosters import _rel_chr, _rel_int

__all__ = ["helper_nba_player_core"]

# Ids are embedded in the core-v2 $ref URL -- parse, never fetch:
#   http://sports.core.api.espn.com/v2/colleges/153?lang=en&region=us
#   http://sports.core.api.espn.com/v2/sports/basketball/leagues/mbb/seasons/2025/teams/153?...
_REF_ID_RE = re.compile(r"/(?:colleges|teams)/(\d+)")

_CORE_COLS: tuple[str, ...] = (
    "athlete_id",
    "guid",
    "uid",
    "slug",
    "type",
    "first_name",
    "last_name",
    "full_name",
    "display_name",
    "short_name",
    "height",
    "display_height",
    "weight",
    "display_weight",
    "age",
    "date_of_birth",
    "birth_city",
    "birth_state",
    "birth_country",
    "jersey",
    "position_id",
    "position_name",
    "position_abbreviation",
    "position_display_name",
    "college_id",
    "current_team_id",
    "headshot_href",
    "experience_years",
    "status_id",
    "status_name",
    "status_type",
    "draft_year",
    "draft_round",
    "draft_selection",
    "active",
)

_INT_COLS: tuple[str, ...] = (
    "age",
    "position_id",
    "college_id",
    "current_team_id",
    "experience_years",
    "status_id",
    "draft_year",
    "draft_round",
    "draft_selection",
)
_FLOAT_COLS: tuple[str, ...] = ("height", "weight")


def _ref_id(node: object) -> int | None:
    """Pull the trailing numeric id out of a core-v2 ``$ref`` URL.

    Returns None when the node is absent or carries no ``/colleges/{id}`` /
    ``/teams/{id}`` segment. Never fetches the ref.
    """
    if not isinstance(node, dict):
        return None
    ref = node.get("$ref")
    if not isinstance(ref, str):
        return None
    m = _REF_ID_RE.search(ref)
    return int(m.group(1)) if m else None


def helper_nba_player_core(payload: dict, *, athlete_id: int | str) -> pl.DataFrame:
    """Project one ESPN core-v2 athlete record into the released player_core row.

    Args:
        payload: One athlete's ``{lg}/player_core/json/{athlete_id}.json`` as a
            dict (ESPN core-v2 ``/athletes/{id}``).
        athlete_id: ESPN athlete id. **Required and not inferred** -- callers
            pass the id from the file path. The released dtype is Int64.

    Returns:
        pl.DataFrame: Exactly one row, always carrying the full documented
        column set (absent fields are null) so callers see a stable schema.
        An empty (zero-column) frame when the payload is empty or not a dict.

        ``current_team_id`` is the athlete's CURRENT team, NOT their team in
        any past season -- see the module docstring. Height/weight/jersey are
        a current snapshot, not era-correct.

    Example:
        Quick start::

            import json
            from sportsdataverse.nba import helper_nba_player_core
            payload = json.load(open("1966.json", encoding="utf-8"))
            df = helper_nba_player_core(payload, athlete_id=1966)
            print(df.select("full_name", "display_height", "weight").row(0))

        Season dimension comes from player_box, not from this payload::

            from sportsdataverse.nba import build_nba_player_identity_lookup
            ids = sorted(int(k) for k in build_nba_player_identity_lookup(player_box))

    See Also:
        * `hoopR`_ -- R sister package for the men's basketball releases.
        * `wehoop`_ -- R sister package for the women's basketball releases.

    .. _hoopR: https://hoopR.sportsdataverse.org
    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    if not isinstance(payload, dict) or not payload:
        return pl.DataFrame()

    position = payload.get("position") or {}
    status = payload.get("status") or {}
    birth = payload.get("birthPlace") or {}
    headshot = payload.get("headshot") or {}
    experience = payload.get("experience") or {}
    draft = payload.get("draft") or {}

    row: dict[str, Any] = {
        "athlete_id": _rel_int(athlete_id),
        "guid": _rel_chr(payload.get("guid")),
        "uid": _rel_chr(payload.get("uid")),
        "slug": _rel_chr(payload.get("slug")),
        "type": _rel_chr(payload.get("type")),
        "first_name": _rel_chr(payload.get("firstName")),
        "last_name": _rel_chr(payload.get("lastName")),
        "full_name": _rel_chr(payload.get("fullName")),
        "display_name": _rel_chr(payload.get("displayName")) or _rel_chr(payload.get("fullName")),
        "short_name": _rel_chr(payload.get("shortName")),
        "height": payload.get("height"),
        "display_height": _rel_chr(payload.get("displayHeight")),
        "weight": payload.get("weight"),
        "display_weight": _rel_chr(payload.get("displayWeight")),
        "age": _rel_int(payload.get("age")),
        "date_of_birth": _rel_chr(payload.get("dateOfBirth")),
        "birth_city": _rel_chr(birth.get("city")),
        "birth_state": _rel_chr(birth.get("state")),
        # College payloads carry a top-level birthCountry; pro payloads nest it.
        "birth_country": _rel_chr(birth.get("country")) or _rel_chr(payload.get("birthCountry")),
        "jersey": _rel_chr(payload.get("jersey")),
        "position_id": _rel_int(position.get("id")),
        "position_name": _rel_chr(position.get("name")),
        "position_abbreviation": _rel_chr(position.get("abbreviation")),
        "position_display_name": _rel_chr(position.get("displayName")),
        "college_id": _ref_id(payload.get("college")),
        "current_team_id": _ref_id(payload.get("team")),
        "headshot_href": _rel_chr(headshot.get("href")),
        "experience_years": _rel_int(experience.get("years")),
        "status_id": _rel_int(status.get("id")),
        "status_name": _rel_chr(status.get("name")),
        "status_type": _rel_chr(status.get("type")),
        "draft_year": _rel_int(draft.get("year")),
        "draft_round": _rel_int(draft.get("round")),
        "draft_selection": _rel_int(draft.get("selection")),
        "active": payload.get("active"),
    }

    df = pl.DataFrame({c: [row.get(c)] for c in _CORE_COLS}, strict=False)
    str_cols = [c for c in _CORE_COLS if c not in _INT_COLS + _FLOAT_COLS + ("athlete_id", "active")]
    return df.with_columns(
        # athlete_id is the join key into player_box / player_season_stats --
        # Int64 here and everywhere, never a float-origin string ("123.0").
        [pl.col("athlete_id").cast(pl.Int64, strict=False)]
        + [pl.col(c).cast(pl.Int32, strict=False) for c in _INT_COLS]
        + [pl.col(c).cast(pl.Float64, strict=False) for c in _FLOAT_COLS]
        + [pl.col("active").cast(pl.Boolean, strict=False)]
        + [pl.col(c).cast(pl.Utf8) for c in str_cols]
    )
