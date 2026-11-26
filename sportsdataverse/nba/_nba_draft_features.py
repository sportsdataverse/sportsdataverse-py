"""Pre-draft college and EuroLeague columns for NBA draft picks (descriptive only).

Attached by ``nba_draft_model`` / ``nba_rookie_projection`` when
``pre_draft_features=True``. The pick curves never read them: a walk-forward
rebuild showed that, once the pick is known, these columns do not improve the
ranking of career or rookie value by a detectable margin.

Links:

- college, id key: the pick's ``(draft_year, overall_pick)`` equals an ESPN
  college bio's ``(draft_year, draft_selection)`` (``load_mbb_player_core``),
  confirmed by a Jaro-Winkler name similarity of at least 0.85. ESPN college
  bios carry draft fields for drafts up to 2021.
- college, name key: the normalized name equals a player-value-spine row
  (``load_mbb_player_value``) in the college season that ends in the draft
  year, and no second row shares it. On the 2006-2021 drafts this key agrees
  with the id key for 0.999 of the picks both cover.
- EuroLeague / EuroCup: the normalized name equals a player in one of the
  three seasons that started before the draft (``euroleague_player_stats``),
  with a single player code.

No birth date is attached. Every birth-date source on hand covers players who
reached the NBA far more often than players who did not, so its coverage
carries the outcome.
"""

from __future__ import annotations

from typing import Callable, Optional

import polars as pl

from sportsdataverse._common_crosswalk_basketball import jaro_winkler, normalize_name

FEATURE_SCHEMA: dict = {
    "college_espn_id": pl.Utf8,
    "college_match": pl.Utf8,
    "col_bpm_last": pl.Float64,
    "col_obpm_last": pl.Float64,
    "col_dbpm_last": pl.Float64,
    "col_min_last": pl.Float64,
    "col_bpm_career": pl.Float64,
    "col_min_career": pl.Float64,
    "col_seasons": pl.Int64,
    "col_team_adj_em": pl.Float64,
    "col_rapm_last": pl.Float64,
    "euro_code": pl.Utf8,
    "euro_min": pl.Float64,
    "euro_pir40": pl.Float64,
    "euro_pts40": pl.Float64,
    "euro_top": pl.Boolean,
}
COLLEGE_YEARS = 5  # college seasons read before the draft (the draft season included)
EURO_YEARS = 3  # EuroLeague / EuroCup seasons read before the draft
RAPM_FIRST_SEASON = 2011
VALUE_FIRST_SEASON = 2006


def _nkey(col: str) -> pl.Expr:
    return pl.col(col).map_elements(normalize_name, return_dtype=pl.Utf8)


def _flip(col: str) -> pl.Expr:
    """EuroLeague ``"LAST, FIRST"`` -> ``"FIRST LAST"``."""
    parts = pl.col(col).str.split_exact(", ", 1)
    return (
        pl.when(pl.col(col).str.contains(", "))
        .then(pl.concat_str([parts.struct.field("field_1"), parts.struct.field("field_0")], separator=" "))
        .otherwise(pl.col(col))
    )


def _seasons(years: list[int], back: int, first: int) -> list[int]:
    return sorted({s for y in years for s in range(y - back + 1, y + 1) if s >= first})


def _safe(fn: Callable[[], pl.DataFrame]) -> pl.DataFrame:
    """A source with no published season (or no answer) contributes nothing."""
    try:
        out = fn()
    except Exception:  # noqa: BLE001 -- descriptive columns: a missing source leaves them null, never raises
        return pl.DataFrame()
    return out if isinstance(out, pl.DataFrame) else pl.DataFrame()


def _college(picks: pl.DataFrame, loaders: dict) -> pl.DataFrame:
    years = sorted(picks["draft_year"].unique().to_list())
    cseasons = _seasons(years, COLLEGE_YEARS, VALUE_FIRST_SEASON)
    if not cseasons:
        return pl.DataFrame()
    core = _safe(lambda: loaders["core"](seasons=cseasons))
    pv = _safe(lambda: loaders["value"](seasons=cseasons))
    if pv.is_empty():
        return pl.DataFrame()

    id_key = pl.DataFrame(schema={"player_id": pl.Utf8, "espn_id": pl.Utf8})
    if not core.is_empty() and "draft_selection" in core.columns:
        bios = (
            core.filter(pl.col("draft_selection").is_not_null())
            .unique(["athlete_id"], keep="first")
            .select(
                pl.col("athlete_id").cast(pl.Utf8).alias("espn_id"),
                pl.col("draft_year").cast(pl.Int64),
                pl.col("draft_selection").cast(pl.Int64).alias("overall_pick"),
                pl.col("full_name").alias("_bio_name"),
            )
        )
        cand = picks.join(bios, on=["draft_year", "overall_pick"], how="inner")
        ok = [
            jaro_winkler(normalize_name(a), normalize_name(b)) >= 0.85
            for a, b in zip(cand["player_name"].to_list(), cand["_bio_name"].to_list())
        ]
        id_key = cand.filter(pl.Series(ok, dtype=pl.Boolean)).unique(["player_id"]).select("player_id", "espn_id")

    pv = pv.with_columns(pl.col("player_id").cast(pl.Utf8).alias("espn_id"), _nkey("player").alias("nkey"))
    cand = picks.with_columns(_nkey("player_name").alias("nkey")).join(
        pv.select("espn_id", "nkey", pl.col("season").cast(pl.Int64).alias("draft_year")),
        on=["nkey", "draft_year"],
        how="inner",
    )
    single = cand.group_by("player_id").len().filter(pl.col("len") == 1).select("player_id")
    name_key = cand.join(single, on="player_id").select("player_id", pl.col("espn_id").alias("_name_id"))

    link = id_key.join(name_key, on="player_id", how="full", coalesce=True).select(
        "player_id",
        pl.coalesce("espn_id", "_name_id").alias("college_espn_id"),
        pl.when(pl.col("espn_id").is_not_null()).then(pl.lit("id")).otherwise(pl.lit("name")).alias("college_match"),
    )
    if link.is_empty():
        return pl.DataFrame()

    hist = (
        link.join(picks.select("player_id", "draft_year"), on="player_id")
        .join(
            pv.select(
                pl.col("espn_id").alias("college_espn_id"),
                pl.col("season").cast(pl.Int64),
                pl.col("team_id").cast(pl.Utf8),
                "min",
                "box_bpm",
                "box_obpm",
                "box_dbpm",
            ),
            on="college_espn_id",
        )
        .filter(pl.col("season") <= pl.col("draft_year"))
        .sort("season")
    )
    last = hist.group_by("player_id").agg(
        pl.col("box_bpm").last().alias("col_bpm_last"),
        pl.col("box_obpm").last().alias("col_obpm_last"),
        pl.col("box_dbpm").last().alias("col_dbpm_last"),
        pl.col("min").last().cast(pl.Float64).alias("col_min_last"),
        ((pl.col("box_bpm") * pl.col("min")).sum() / pl.col("min").sum()).alias("col_bpm_career"),
        pl.col("min").sum().cast(pl.Float64).alias("col_min_career"),
        pl.len().cast(pl.Int64).alias("col_seasons"),
        pl.col("team_id").last().alias("_team"),
        pl.col("season").last().alias("_season"),
    )
    ratings = _safe(lambda: loaders["ratings"](seasons=cseasons))
    if not ratings.is_empty():
        last = last.join(
            ratings.select(
                pl.col("team_id").cast(pl.Utf8).alias("_team"),
                pl.col("season").cast(pl.Int64).alias("_season"),
                pl.col("adj_em").alias("col_team_adj_em"),
            ),
            on=["_team", "_season"],
            how="left",
        )
    out = link.join(last.drop("_team", "_season"), on="player_id", how="left")

    rseasons = _seasons(years, COLLEGE_YEARS, RAPM_FIRST_SEASON)
    if rseasons:
        cw = _safe(lambda: loaders["crosswalk"](seasons=rseasons))
        rapm = _safe(lambda: loaders["rapm"](seasons=rseasons))
        if not cw.is_empty() and not rapm.is_empty():
            cw = (
                cw.select(
                    pl.col("espn_athlete_id").cast(pl.Utf8).alias("college_espn_id"),
                    pl.col("season").cast(pl.Int64),
                    pl.col("ncaa_player_id").cast(pl.Utf8),
                )
                .drop_nulls("ncaa_player_id")
                .unique(["college_espn_id", "season"])
            )
            r = rapm.select(
                pl.col("player_id").cast(pl.Utf8).alias("ncaa_player_id"), pl.col("season").cast(pl.Int64), "rapm_net"
            )
            rap = (
                out.select("player_id", "college_espn_id")
                .join(picks.select("player_id", "draft_year"), on="player_id")
                .join(cw, on="college_espn_id")
                .filter(pl.col("season") <= pl.col("draft_year"))
                .join(r, on=["ncaa_player_id", "season"])
                .sort("season")
                .group_by("player_id")
                .agg(pl.col("rapm_net").last().alias("col_rapm_last"))
            )
            out = out.join(rap, on="player_id", how="left")
    return out


def _euro(picks: pl.DataFrame, loaders: dict) -> pl.DataFrame:
    years = sorted(picks["draft_year"].unique().to_list())
    starts = sorted({s for y in years for s in range(y - EURO_YEARS, y)})
    frames = []
    for comp in ("E", "U"):
        for s in starts:
            df = _safe(lambda c=comp, s=s: loaders["euro_stats"](c, s))
            if not df.is_empty():
                frames.append(df.with_columns(pl.lit(s).alias("season_start"), pl.lit(comp).alias("competition")))
    if not frames:
        return pl.DataFrame()
    stats = pl.concat(frames, how="diagonal_relaxed").with_columns(_flip("player_name").alias("_n"))
    stats = stats.with_columns(_nkey("_n").alias("nkey"))
    es = (
        picks.with_columns(_nkey("player_name").alias("nkey"))
        .select("player_id", "nkey", "draft_year")
        .join(
            stats.select(
                "nkey",
                pl.col("player_code").cast(pl.Utf8),
                "season_start",
                "competition",
                pl.col("minutes_played").cast(pl.Float64),
                pl.col("pir").cast(pl.Float64),
                pl.col("points_scored").cast(pl.Float64),
            ),
            on="nkey",
        )
        .filter(pl.col("season_start").is_between(pl.col("draft_year") - EURO_YEARS, pl.col("draft_year") - 1))
    )
    single = es.group_by("player_id").agg(pl.col("player_code").n_unique().alias("n")).filter(pl.col("n") == 1)
    return (
        es.join(single.select("player_id"), on="player_id")
        .group_by("player_id")
        .agg(
            pl.col("player_code").first().alias("euro_code"),
            pl.col("minutes_played").sum().alias("euro_min"),
            (pl.col("pir").sum() / pl.col("minutes_played").sum().clip(1.0) * 40).alias("euro_pir40"),
            (pl.col("points_scored").sum() / pl.col("minutes_played").sum().clip(1.0) * 40).alias("euro_pts40"),
            (pl.col("competition") == "E").any().alias("euro_top"),
        )
    )


def _default_loaders() -> dict:
    from sportsdataverse.basketball.euroleague import euroleague_player_stats
    from sportsdataverse.mbb.mbb_loaders import (
        load_mbb_player_core,
        load_mbb_player_crosswalk,
        load_mbb_player_value,
        load_mbb_ratings,
        load_ncaa_mbb_rapm,
    )

    return {
        "core": load_mbb_player_core,
        "value": load_mbb_player_value,
        "ratings": load_mbb_ratings,
        "crosswalk": load_mbb_player_crosswalk,
        "rapm": load_ncaa_mbb_rapm,
        "euro_stats": lambda comp, start: euroleague_player_stats(
            competition_code=comp, season_code=f"{comp}{start}", statistic_mode="Accumulated", limit="2000"
        ),
    }


def pre_draft_features(picks: pl.DataFrame, loaders: Optional[dict] = None) -> pl.DataFrame:
    """Left-join the pre-draft college and EuroLeague columns onto ``picks``.

    Args:
        picks: Frame with ``player_id:Utf8``, ``draft_year:Int64``,
            ``overall_pick:Int64`` and ``player_name:Utf8``.
        loaders: Source callables keyed ``core``, ``value``, ``ratings``,
            ``crosswalk``, ``rapm``, ``euro_stats``. ``None`` uses the release
            loaders and the EuroLeague API. Tests pass captured slices.

    Returns:
        ``picks`` plus every :data:`FEATURE_SCHEMA` column; a column is null
        when its source has no link for that pick.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba._nba_draft_features import pre_draft_features
            picks = pl.DataFrame({"player_id": ["1629627"], "draft_year": [2019],
                                  "overall_pick": [1], "player_name": ["Zion Williamson"]})
            pre_draft_features(picks).select("player_name", "col_bpm_last")
    """
    for k in ("player_id", "player_name"):
        if picks.schema.get(k) != pl.Utf8:
            raise ValueError(f"picks.{k} must be Utf8, got {picks.schema.get(k)}")
    src = loaders or _default_loaders()
    out = picks
    for block in (_college(picks, src), _euro(picks, src)):
        if not block.is_empty():
            assert block.schema["player_id"] == out.schema["player_id"]
            out = out.join(block, on="player_id", how="left")
    for col, dtype in FEATURE_SCHEMA.items():
        out = out.with_columns((pl.col(col) if col in out.columns else pl.lit(None)).cast(dtype).alias(col))
    return out
