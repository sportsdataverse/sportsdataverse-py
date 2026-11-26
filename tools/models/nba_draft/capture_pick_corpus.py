"""Capture the pick-curve corpus and the 2019 pre-draft replay slices (release assets + EuroLeague API).

Writes, under ``tests/fixtures/nba_draft/``:

- ``pick_corpus.parquet``: every drafted player, draft classes 2003-2019.
  Columns: ``player_id:Utf8``, ``draft_year``, ``overall_pick``,
  ``player_name``, ``career_value``, ``rookie_value``, ``soph_value``,
  ``rookie_min``.
  - Picks come from the ``nba_stats_draft`` release. That release stamps a
    draft with the END year of the season it opens, so the 2013 draft reads
    2014 and is shifted back here.
  - Labels are the shipped box-value formula (``LEAGUE_CONSTANTS["nba"]``,
    the same per-player possession estimate as ``fit_box_value.py``) on the
    ``nba_stats_player_season_stats`` release (regular season, totals, base),
    counting seasons after the draft only.
  - ``rookie_*`` is the first such season and ``soph_value`` the second.
  - A pick who never played gets 0.
  - Classes after 2019 are left out on purpose: 2020-2023 were spent as the rebuild's
    test windows and 2024-2025 is the unscored rookie reserve.
- ``pre_draft_2019/*.parquet``: each pre-draft source cut to the rows the 2019
  picks can match, plus ``expected.parquet`` (the live
  ``pre_draft_features`` output), and ``draft_raw.parquet`` (the 2019 class as
  the ``nba_stats_draft`` release publishes it), for offline replay tests.

Run: ``uv run python tools/models/nba_draft/capture_pick_corpus.py`` (network: GitHub releases +
api-live.euroleague.net; never stats.nba.com).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.nba._nba_draft_features import _default_loaders, _flip, _nkey, pre_draft_features
from sportsdataverse.nba.nba_draft_constants import BOX_VALUE_FEATURES, get_constants
from sportsdataverse.nba.nba_loaders import load_nba_stats_draft, load_nba_stats_player_season_stats

FIXTURE_DIR = Path("tests/fixtures/nba_draft")
FIRST_CLASS, LAST_CLASS = 2003, 2019
REPLAY_CLASS = 2019


def draft_picks(classes: list[int]) -> pl.DataFrame:
    raw = load_nba_stats_draft(seasons=classes)
    return raw.filter(pl.col("overall_pick").is_not_null() & (pl.col("overall_pick") > 0)).select(
        pl.col("person_id").cast(pl.Int64).cast(pl.Utf8).alias("player_id"),
        (pl.col("season").cast(pl.Int64) - 1).alias("draft_year"),
        pl.col("overall_pick").cast(pl.Int64),
        "player_name",
    )


def season_values(seasons: list[int]) -> pl.DataFrame:
    ss = load_nba_stats_player_season_stats(seasons=seasons).filter(
        (pl.col("season_type") == "regular-season")
        & (pl.col("per_mode") == "totals")
        & (pl.col("measure_type") == "base")
    )
    assert ss.group_by("player_id", "season").len()["len"].max() == 1, "one row per player-season expected"
    pos = pl.col("fga") + 0.44 * pl.col("fta") + pl.col("tov")
    p = pos.clip(1.0, None)
    rates = ss.with_columns(
        (pl.col("pts") / p * 100).alias("pts100"),
        (pl.col("reb") / p * 100).alias("reb100"),
        (pl.col("ast") / p * 100).alias("ast100"),
        (pl.col("stl") / p * 100).alias("stl100"),
        (pl.col("blk") / p * 100).alias("blk100"),
        (pl.col("tov") / p * 100).alias("tov100"),
        (pl.col("pts") / (2 * (pl.col("fga") + 0.44 * pl.col("fta")).clip(1.0, None))).alias("ts_pct"),
        (pos / pl.col("min").clip(1.0, None) * 100).alias("usg"),
    )
    k = get_constants("nba")
    val = pl.lit(k.box_value_coef[0])
    for f, c in zip(BOX_VALUE_FEATURES, k.box_value_coef[1:], strict=True):
        val = val + pl.col(f).fill_null(0.0) * c
    return rates.select(
        pl.col("player_id").cast(pl.Int64).cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
        pl.col("min").cast(pl.Float64),
        ((val - k.replacement) * pl.col("min") / 1000.0).alias("vorp"),
    )


def corpus() -> pl.DataFrame:
    picks = draft_picks(list(range(FIRST_CLASS, LAST_CLASS + 1)))
    want = list(range(FIRST_CLASS, 2026))  # through the 2025-26 season
    sv = season_values(want)
    assert picks.schema["player_id"] == sv.schema["player_id"] == pl.Utf8
    # a skipped season asset would shift every later season into "rookie" / "soph" and shrink careers;
    # the loader takes START years and stamps each row with the season's END year
    missing = sorted({y + 1 for y in want} - set(sv["season"].unique().to_list()))
    assert not missing, f"season assets missing from the capture: {missing}"
    post = (
        sv.join(picks.select("player_id", "draft_year"), on="player_id")
        .filter(pl.col("season") > pl.col("draft_year"))
        .sort("season")
    )
    agg = post.group_by("player_id").agg(
        pl.col("vorp").sum().alias("career_value"),
        pl.col("vorp").first().alias("rookie_value"),
        pl.col("vorp").get(1, null_on_oob=True).alias("soph_value"),
        pl.col("min").first().alias("rookie_min"),
    )
    return (
        picks.join(agg, on="player_id", how="left")
        .with_columns(pl.col(c).fill_null(0.0) for c in ("career_value", "rookie_value", "soph_value", "rookie_min"))
        .sort("draft_year", "overall_pick")
    )


def replay_slices(picks: pl.DataFrame) -> None:
    out = FIXTURE_DIR / "pre_draft_2019"
    out.mkdir(exist_ok=True)
    real = _default_loaders()
    names = picks.with_columns(_nkey("player_name").alias("nkey"))["nkey"].implode()
    recorded: dict[str, list[pl.DataFrame]] = {}

    def keep(key: str, df: pl.DataFrame) -> pl.DataFrame:
        recorded.setdefault(key, []).append(df)
        return df

    rec = {
        "core": lambda seasons: keep("core", real["core"](seasons=seasons)),
        "value": lambda seasons: keep("value", real["value"](seasons=seasons)),
        "ratings": lambda seasons: keep("ratings", real["ratings"](seasons=seasons)),
        "crosswalk": lambda seasons: keep("crosswalk", real["crosswalk"](seasons=seasons)),
        "rapm": lambda seasons: keep("rapm", real["rapm"](seasons=seasons)),
        "euro_stats": lambda comp, start: keep(
            "euro_stats",
            real["euro_stats"](comp, start).with_columns(pl.lit(comp).alias("_comp"), pl.lit(start).alias("_start")),
        ),
    }
    expected = pre_draft_features(picks, rec)
    expected.write_parquet(out / "expected.parquet")

    def cat(key: str) -> pl.DataFrame:
        return pl.concat(recorded[key], how="diagonal_relaxed")

    espn = expected["college_espn_id"].drop_nulls().implode()
    core = cat("core")
    core = core.filter(
        pl.col("athlete_id").cast(pl.Utf8).is_in(espn)
        | (pl.col("draft_year") == REPLAY_CLASS)
        | _nkey("full_name").is_in(names)
    )
    value = cat("value").filter(_nkey("player").is_in(names) | pl.col("player_id").cast(pl.Utf8).is_in(espn))
    teams = value["team_id"].cast(pl.Utf8).unique().implode()
    ratings = cat("ratings").filter(pl.col("team_id").cast(pl.Utf8).is_in(teams))
    cw = cat("crosswalk").filter(pl.col("espn_athlete_id").cast(pl.Utf8).is_in(espn))
    rapm = cat("rapm").filter(pl.col("player_id").cast(pl.Utf8).is_in(cw["ncaa_player_id"].drop_nulls().implode()))
    euro = cat("euro_stats").with_columns(_flip("player_name").alias("_n"))
    euro = euro.filter(_nkey("_n").is_in(names)).drop("_n")
    for key, df in (
        ("core", core),
        ("value", value),
        ("ratings", ratings),
        ("crosswalk", cw),
        ("rapm", rapm),
        ("euro_stats", euro),
    ):
        df.write_parquet(out / f"{key}.parquet")
        print(f"  pre_draft_2019/{key}.parquet: {df.height} rows")
    picks.write_parquet(out / "picks.parquet")
    load_nba_stats_draft(seasons=[REPLAY_CLASS]).write_parquet(out / "draft_raw.parquet")


def main() -> None:
    c = corpus()
    c.write_parquet(FIXTURE_DIR / "pick_corpus.parquet")
    print(f"pick_corpus.parquet: {c.height} picks, classes {c['draft_year'].min()}-{c['draft_year'].max()}")
    replay_slices(draft_picks([REPLAY_CLASS]))


if __name__ == "__main__":
    main()
