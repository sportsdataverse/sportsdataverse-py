"""ESPN WBB play-by-play producer -- polars port of ``wehoop:::helper_espn_wbb_pbp``.

Source: ``wehoop/R/espn_wbb_data.R`` lines 2763-3160 (wehoop 3.0.0). Takes one
game's stored ``final.json`` payload (the wehoop-wbb-raw scraper persists the
*processed* summary: plays are flat dicts with dotted/camelCase keys and the
engineered clock/spread/lead-lag features already baked in) and returns the
per-game plays frame published to the ``espn_womens_college_basketball_pbp``
release. NOT the same lineage as ``wbb_pbp.helper_wbb_pbp`` (the interactive
``espn_wbb_pbp`` pipeline) -- this is the release-parity producer.

The R-released parquet is the parity oracle: dtypes and column order mirror it
exactly (R integer == Int32), with ONE deliberate dtype improvement -- the play
``id`` is emitted Int64, not R's precision-losing Float64 (see ``_INT64_COLS``).
The four ``coordinate_*`` columns the creation script appends as season-level
NA fallback when a season ships no coordinates are added here per-game
(identical union outcome, no season-level special case needed).

Documented deviations from R (both on compat paths current payloads skip):
the R away-first ``id_vars`` branch plucks the DARK logo for ``homeTeamLogo``
(``logos[[2]]``, a copy-paste bug) and hard-errors on one-logo teams -- this
port normalizes both teams to ``logos[0]``/``logos[1]`` with null fallbacks;
and the nested-``participants`` compat unnest emits no ``play_id`` companion
column and tolerates 0-3 participants where R assumes exactly 2.

Running score: ``home_score``/``away_score`` are rebuilt from the scoring plays and kept
only when the rebuild reproduces the header final; otherwise ESPN's per-play values are
kept, and ``score_source`` (``"rebuilt"``/``"espn"``) says which. The same rule is ported
to the hoopR / wehoop R helpers; their PRs land before any producer republish (see
``_repair_running_score``).
"""

from __future__ import annotations

from typing import Any

import polars as pl

from sportsdataverse._espn_basketball_pbp import hoopr_coordinates
from sportsdataverse.dl_utils import underscore
from sportsdataverse.wbb.wbb_team_box import _game_datetime, _to_int

# Released-parquet dtype contract (union of the published seasons' schemas).
# Columns not listed pass through with their inferred dtype.
_INT32_COLS: tuple[str, ...] = (
    "game_play_number",
    "sequence_number",
    "type_id",
    "away_score",
    "home_score",
    "period_number",
    "score_value",
    "team_id",
    "athlete_id_1",
    "athlete_id_2",
    "athlete_id_3",
    "game_id",
    "season",
    "season_type",
    "home_team_id",
    "away_team_id",
    "qtr",
    "clock_minutes",
    "clock_seconds",
    "half",
    "game_half",
    "lead_qtr",
    "lead_half",
    "start_quarter_seconds_remaining",
    "start_half_seconds_remaining",
    "start_game_seconds_remaining",
    "end_quarter_seconds_remaining",
    "end_half_seconds_remaining",
    "end_game_seconds_remaining",
    "period",
    "lag_qtr",
    "lag_half",
    "points_attempted",
)
_FLOAT64_COLS: tuple[str, ...] = (
    "game_spread",
    "home_team_spread",
    "coordinate_x_raw",
    "coordinate_y_raw",
    "coordinate_x",
    "coordinate_y",
)
# DELIBERATE divergence from the R releases: R/jsonlite has no int64, so the
# published `id` is Float64 and LOSES PRECISION above 2^53 -- adjacent play ids
# (~4e17) round to the same double and collide. The stored payload carries the
# id as a true integer, so the Python producer emits Int64 and keeps it exact.
_INT64_COLS: tuple[str, ...] = ("id",)

# Appended (as all-null Float64) only when a payload ships no coordinates at
# all. Order matters: it is the order the R creation script's season-level
# fallback appends them in (espn_wnba_01_pbp_creation.R:100-106), and these
# land at the end of the frame.
_COORD_COLS: tuple[str, ...] = (
    "coordinate_x",
    "coordinate_y",
    "coordinate_x_raw",
    "coordinate_y_raw",
)

# Free-throw play types: NBA/WNBA "Free Throw - 1 of 2", college "MadeFreeThrow".
_FREE_THROW = r"(?i)free ?throw"
# Builder columns the running-score rebuild reads; a frame missing any keeps ESPN's scores.
_SCORE_INPUTS: tuple[str, ...] = ("scoring_play", "score_value", "team_id", "type_text", "home_score", "away_score")

__all__ = ["helper_wbb_play_by_play"]


def _clean_name(name: str) -> str:
    """janitor::clean_names parity for the stored dotted/camelCase play keys."""
    return underscore(name.replace(".", "_"))


def _home_team_id(competitors: list[dict[str, Any]]) -> int | None:
    """The home competitor's team id (R: homeAway1-branched pluck chain)."""
    for c in competitors:
        if c.get("homeAway") == "home":
            return _to_int((c.get("team") or {}).get("id"))
    return None


def _id_vars(competitors: list[dict[str, Any]]) -> dict[str, Any]:
    """R id_vars fallback block -- only bound when plays lack homeTeamName.

    The wehoop-wbb-raw payloads always carry the team columns, so this is a
    compat path for pre-processed payload shapes.
    """
    home = next((c for c in competitors if c.get("homeAway") == "home"), {})
    away = next((c for c in competitors if c.get("homeAway") != "home"), {})
    out: dict[str, Any] = {}
    for prefix, c in (("home", home), ("away", away)):
        team = c.get("team") or {}
        logos = team.get("logos") or []
        records = c.get("record") or []
        out[f"{prefix}TeamId"] = _to_int(team.get("id"))
        out[f"{prefix}TeamMascot"] = team.get("name")
        out[f"{prefix}TeamName"] = team.get("location")
        out[f"{prefix}TeamAbbrev"] = team.get("abbreviation")
        out[f"{prefix}TeamLogo"] = (logos[0] or {}).get("href") if logos else None
        out[f"{prefix}TeamLogoDark"] = (logos[1] or {}).get("href") if len(logos) > 1 else None
        out[f"{prefix}TeamFullName"] = team.get("displayName")
        out[f"{prefix}TeamColor"] = team.get("color")
        out[f"{prefix}TeamAlternateColor"] = team.get("alternateColor")
        out[f"{prefix}TeamScore"] = _to_int(c.get("score"))
        out[f"{prefix}TeamWinner"] = c.get("winner")
        out[f"{prefix}TeamRecord"] = (records[0] or {}).get("summary") if records else None
    return out


def helper_wbb_play_by_play(final: dict) -> pl.DataFrame:
    """Parse one game's stored payload into the released play-by-play frame.

    Faithful polars port of ``wehoop:::helper_espn_wbb_pbp``
    (``wehoop/R/espn_wbb_data.R:2763``). Returns one row per play whose
    column set, order, and dtypes match the R-released
    ``espn_womens_college_basketball_pbp`` parquet for that game's season.

    Args:
        final: One game's stored payload (the ``final.json`` the
            ``wehoop-wbb-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: One row per play. Empty (zero-column) frame when the
        payload has ``playByPlaySource == "none"`` or 10 or fewer plays
        (R guard) -- season builders skip empty frames.
        ``home_score`` / ``away_score`` are rebuilt from the scoring plays
        when that reproduces the header's official final, otherwise they are
        ESPN's per-play values; the last column, ``score_source``
        (``"rebuilt"`` / ``"espn"``), says which. Select it by name: a
        release built from this frame can carry columns after it.

    Example:
        Quick start::

            import json
            from sportsdataverse.wbb import helper_wbb_play_by_play
            final = json.load(open("401700473.json", encoding="utf-8"))
            df = helper_wbb_play_by_play(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("shooting_play") == True).height

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return _basketball_play_by_play(final, int32_cols=_INT32_COLS, float64_cols=_FLOAT64_COLS)


def _basketball_play_by_play(
    final: dict,
    *,
    int32_cols: tuple[str, ...],
    float64_cols: tuple[str, ...],
) -> pl.DataFrame:
    """League-parameterized core shared by the WBB/WNBA release producers.

    The wehoop WBB and WNBA pbp helpers are semantically identical except for
    the released dtype contract: WNBA's sub-second game clocks keep
    ``clock_seconds`` and the six ``*_seconds_remaining`` columns Float64,
    where WBB releases them Int32 (and WNBA names a third participant athlete
    column, which this core already emits for both). Callers pass their
    league's cast tuples; everything else is shared verbatim. The last step is
    :func:`_repair_running_score`, shared by all four leagues.
    """
    header = final.get("header") or {}
    competitions = header.get("competitions") or []
    if not competitions:
        return pl.DataFrame()
    comp = competitions[0]
    plays_raw = final.get("plays") or []
    if comp.get("playByPlaySource") == "none" or len(plays_raw) <= 10:
        return pl.DataFrame()
    competitors = comp.get("competitors") or []

    game_datetime = _game_datetime(comp["date"])
    season = header.get("season") or {}
    home_id = _home_team_id(competitors)

    # Compat: old nested participants[] -> dotted keys (R unnest_wider branch).
    plays: list[dict[str, Any]] = []
    for p in plays_raw:
        parts = p.get("participants")
        if isinstance(parts, list):
            p = {k: v for k, v in p.items() if k != "participants"}
            for i, part in enumerate(parts[:3]):
                p[f"participants.{i}.athlete.id"] = ((part or {}).get("athlete") or {}).get("id")
        # R round-trips game_json through jsonlite::toJSON (default digits = 4)
        # before plucking plays, so every double is rounded to 4 decimal
        # places. Visible on WNBA's fractional clocks, where the stored
        # payload carries float32-widened values (57.900001525878906 -> 57.9);
        # a no-op for WBB's integer clocks and one-decimal coordinates.
        p = {k: (round(v, 4) if isinstance(v, float) else v) for k, v in p.items()}
        plays.append(p)

    # First-seen key order across all plays (R: fromJSON record-union order).
    order: dict[str, None] = {}
    for p in plays:
        for k in p:
            order.setdefault(k, None)
    # R: select(-any_of(c("athlete.id", "athlete_id")))
    for drop in ("athlete.id", "athlete_id"):
        order.pop(drop, None)
    df = pl.DataFrame({k: [p.get(k) for p in plays] for k in order}, strict=False)

    # Coordinate transform (R lines 3058-3090), shared with the four pbp processors.
    has_coords = "coordinate.x" in df.columns and "coordinate.y" in df.columns
    df = hoopr_coordinates(df, home_id)

    # R id_vars fallback: bound only when plays lack the team columns.
    if "homeTeamName" not in df.columns:
        df = df.with_columns([pl.lit(v).alias(k) for k, v in _id_vars(competitors).items()])

    # janitor::clean_names + game/season columns (overwrite in place; the two
    # date columns are new and append) + participants -> athlete_id renames.
    df = df.rename({c: _clean_name(c) for c in df.columns})
    df = df.with_columns(
        pl.lit(_to_int(header.get("id"))).alias("game_id"),
        pl.lit(season.get("year")).alias("season"),
        pl.lit(season.get("type")).alias("season_type"),
        pl.lit(game_datetime.date()).alias("game_date"),
        pl.lit(game_datetime).cast(pl.Datetime("us", "America/New_York")).alias("game_date_time"),
    )
    df = df.rename(
        {
            old: new
            for old, new in (
                ("participants_0_athlete_id", "athlete_id_1"),
                ("participants_1_athlete_id", "athlete_id_2"),
                ("participants_2_athlete_id", "athlete_id_3"),
            )
            if old in df.columns
        }
    )

    # Creation-script parity: seasons without coordinates ship the four
    # columns as all-null Float64 (appended after the date columns).
    if not has_coords:
        df = df.with_columns([pl.lit(None, dtype=pl.Float64).alias(c) for c in _COORD_COLS])

    # Released dtype contract (except `id` -- see _INT64_COLS).
    df = df.with_columns(
        [pl.col(c).cast(pl.Int32, strict=False) for c in int32_cols if c in df.columns]
        + [pl.col(c).cast(pl.Int64, strict=False) for c in _INT64_COLS if c in df.columns]
        + [pl.col(c).cast(pl.Float64, strict=False) for c in float64_cols if c in df.columns]
    )
    return _repair_running_score(df, competitors)


def _repair_running_score(df: pl.DataFrame, competitors: list[dict[str, Any]]) -> pl.DataFrame:
    """Rebuild the running score from the scoring plays when that reproduces the official final.

    ESPN writes each play's ``homeScore``/``awayScore`` from the live scoreboard at the moment
    the row is keyed. A play keyed late (a technical free throw entered after play resumed)
    gets the right score on its own row, but rows already written are never re-based, so the
    feed runs backwards and can end short of the final (NBA 401766458: 119-95 against 120-95).
    College neutral-site feeds can carry the away team's score in ``homeScore`` for a whole game.

    The rebuild sums each scoring play's points in ESPN's array order (never re-sorted:
    ``sequence_number`` is not chronological), crediting the header's home or away team by
    ``team_id``. A made free throw shipped with ``scoreValue`` 0 (NBA/WNBA 2013-2019) counts 1.
    The rebuilt pair replaces ESPN's on every row only when its last row equals the header
    final; otherwise every row keeps ESPN's value. ``score_source`` (last column) records which:
    ``"rebuilt"`` or ``"espn"``.

    Args:
        df: The builder frame after the released dtype casts.
        competitors: ``header.competitions[0].competitors`` of the same payload.

    Returns:
        pl.DataFrame: ``df`` with ``home_score`` / ``away_score`` (Int32) rebuilt or untouched,
        plus ``score_source`` (String).
    """
    home = next((c for c in competitors if c.get("homeAway") == "home"), {})
    away = next((c for c in competitors if c.get("homeAway") != "home"), {})
    home_id = _to_int((home.get("team") or {}).get("id"))
    away_id = _to_int((away.get("team") or {}).get("id"))
    final = (_to_int(home.get("score")), _to_int(away.get("score")))
    espn = df.with_columns(pl.lit("espn").alias("score_source"))
    if home_id is None or away_id is None or None in final or not set(_SCORE_INPUTS).issubset(df.columns):
        return espn
    points = (
        pl.when(pl.col("scoring_play") == True)
        .then(
            pl.when(pl.col("score_value") > 0)
            .then(pl.col("score_value"))
            .when(pl.col("type_text").str.contains(_FREE_THROW))
            .then(1)
            .otherwise(0)
        )
        .otherwise(0)
        .fill_null(0)
        .cast(pl.Int32)
    )
    rebuilt = df.select(
        pl.when(pl.col("team_id") == home_id).then(points).otherwise(0).cum_sum().cast(pl.Int32).alias("home_score"),
        pl.when(pl.col("team_id") == away_id).then(points).otherwise(0).cum_sum().cast(pl.Int32).alias("away_score"),
    )
    if rebuilt.row(rebuilt.height - 1) != final:
        return espn
    return df.with_columns([*rebuilt.get_columns(), pl.lit("rebuilt").alias("score_source")])
