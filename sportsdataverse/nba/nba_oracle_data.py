"""Loaders for external published-metric CSVs used as validation-harness oracles.

Five published-metric families (Ryan Davis RAPM, Dunks & Threes EPM, LEBRON
season + daily, DARKO DPM, Dunks & Threes counting stats/ewins) are parsed
into tidy polars frames here so
:func:`sportsdataverse.nba.nba_model_validation.external_validity` can
correlate the model zoo's own ratings against them (Oracle 5, concurrent
validity). Every loader is a **pure CSV parser**: it takes a filesystem
``path``, returns a tidy frame, and never fetches network data. Callers
point ``path=`` at files under a directory named by the
``SDV_PY_NBA_ORACLE_DIR`` environment variable -- never hardcode the
directory (see ``tests/nba/test_nba_oracle_data.py`` for the gate pattern).
"""

from __future__ import annotations

import csv
import re
import unicodedata

import polars as pl

__all__ = [
    "normalize_player_name",
    "RAPM_ORACLE_SCHEMA",
    "load_rapm_ryan_davis",
    "EPM_ORACLE_SCHEMA",
    "load_epm",
    "LEBRON_SEASON_ORACLE_SCHEMA",
    "load_lebron_season",
    "LEBRON_DAILY_ORACLE_SCHEMA",
    "load_lebron_daily",
    "DARKO_DPM_ORACLE_SCHEMA",
    "load_darko_dpm",
]


def normalize_player_name(name: str) -> str:
    """Fold a player display name to a join-safe key.

    Lower-cases, strips diacritics (``"Jokić"`` -> ``"jokic"`` -- the real
    stats.nba.com feed spells Nikola Jokic's name with the Serbian ``ć``,
    while the DARKO/D&T CSVs use plain ASCII), drops periods/apostrophes/
    hyphens, collapses internal whitespace, and strips a trailing
    Jr./Sr./II/III/IV suffix. Two names normalize equal iff they refer to
    the same join key under this scheme -- it is NOT guaranteed globally
    unique (rare true duplicate full names are a known, accepted residual;
    ``external_validity``'s ``coverage_pct`` surfaces the effect rather
    than hiding it).

    Args:
        name: A raw display name, e.g. ``"Nikola Jokić"`` or ``"A.J. Green"``.

    Returns:
        The normalized key, e.g. ``"nikola jokic"``, ``"aj green"``. Empty
        string in, empty string out (never raises).

    Example:
        Diacritic + suffix folding::

            from sportsdataverse.nba.nba_oracle_data import normalize_player_name
            assert normalize_player_name("Nikola Jokić") == normalize_player_name("Nikola Jokic")
            assert normalize_player_name("Gary Trent Jr.") == normalize_player_name("Gary Trent")
    """
    if not name:
        return ""
    decomposed = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(c for c in decomposed if not unicodedata.combining(c))
    lowered = ascii_only.lower().strip()
    no_punct = re.sub(r"[.'\-]", "", lowered)
    collapsed = re.sub(r"\s+", " ", no_punct).strip()
    return re.sub(r"\s+(jr|sr|ii|iii|iv)$", "", collapsed)


#: Tidy schema for :func:`load_rapm_ryan_davis`.
RAPM_ORACLE_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Int64,
    "player_name": pl.Utf8,
    "season": pl.Utf8,
    "LA_RAPM": pl.Float64,
    "RAPM": pl.Float64,
    "RA_EFG": pl.Float64,
    "RA_FTR": pl.Float64,
    "RA_ORBD": pl.Float64,
    "RA_TOV": pl.Float64,
}


def load_rapm_ryan_davis(path: str) -> pl.DataFrame:
    """Parse a Ryan Davis published RAPM CSV (single-season or multi-year window).

    Serves BOTH real files -- ``rapm_ryan_davis.csv`` (``season`` like
    ``"2009-10"``) and ``rapm_multi_ryan_davis.csv`` (``season`` like
    ``"2011-16"``, a multi-year decay window) -- since they share an
    identical header. Only the combined (not per-side ``__Off``/``__Def``)
    rating columns are kept, matching the model zoo's combined-rating
    convention (``nba_rapm``'s ``rapm`` column, not separate offense/defense).

    Args:
        path: Filesystem path to a Ryan Davis RAPM CSV.

    Returns:
        Frame with schema :data:`RAPM_ORACLE_SCHEMA`. Zero rows (with that
        schema) when the file has a header but no data rows.

    Raises:
        FileNotFoundError: If ``path`` does not exist.

    Example:
        Load and filter to one season::

            import polars as pl
            from sportsdataverse.nba.nba_oracle_data import load_rapm_ryan_davis
            oracle = load_rapm_ryan_davis(f"{oracle_dir}/rapm_ryan_davis.csv")
            season = oracle.filter(pl.col("season") == "2022-23")
    """
    raw = pl.read_csv(path)
    if raw.is_empty():
        return pl.DataFrame(schema=RAPM_ORACLE_SCHEMA)
    return raw.select(
        pl.col("playerId").cast(pl.Int64).alias("player_id"),
        pl.col("playerName").cast(pl.Utf8).alias("player_name"),
        pl.col("season").cast(pl.Utf8),
        pl.col("LA_RAPM").cast(pl.Float64),
        pl.col("RAPM").cast(pl.Float64),
        pl.col("RA_EFG").cast(pl.Float64),
        pl.col("RA_FTR").cast(pl.Float64),
        pl.col("RA_ORBD").cast(pl.Float64),
        pl.col("RA_TOV").cast(pl.Float64),
    )


#: Tidy schema for :func:`load_epm`.
EPM_ORACLE_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Int64,
    "season": pl.Int64,
    "player_name": pl.Utf8,
    "team": pl.Utf8,
    "oepm": pl.Float64,
    "depm": pl.Float64,
    "epm": pl.Float64,
}


def load_epm(path: str) -> pl.DataFrame:
    """Parse a Dunks & Threes EPM CSV (``{season}_EPM_data.csv``).

    Args:
        path: Filesystem path to a D&T EPM CSV.

    Returns:
        Frame with schema :data:`EPM_ORACLE_SCHEMA`. Zero rows (with that
        schema) when the file has a header but no data rows.

    Raises:
        FileNotFoundError: If ``path`` does not exist.

    Example:
        Load one season's EPM::

            from sportsdataverse.nba.nba_oracle_data import load_epm
            oracle = load_epm(f"{oracle_dir}/2025_EPM_data.csv")
    """
    raw = pl.read_csv(path)
    if raw.is_empty():
        return pl.DataFrame(schema=EPM_ORACLE_SCHEMA)
    return raw.select(
        pl.col("nba_id").cast(pl.Int64).alias("player_id"),
        pl.col("season").cast(pl.Int64),
        pl.col("name").cast(pl.Utf8).alias("player_name"),
        pl.col("team").cast(pl.Utf8),
        pl.col("oepm").cast(pl.Float64),
        pl.col("depm").cast(pl.Float64),
        pl.col("epm").cast(pl.Float64),
    )


#: Tidy schema for :func:`load_lebron_season`.
LEBRON_SEASON_ORACLE_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Int64,
    "player_name": pl.Utf8,
    "seasons": pl.Utf8,
    "team": pl.Utf8,
    "lebron": pl.Float64,
    "o_lebron": pl.Float64,
    "d_lebron": pl.Float64,
    "war": pl.Float64,
}

#: Tidy schema for :func:`load_lebron_daily`.
LEBRON_DAILY_ORACLE_SCHEMA: dict[str, pl.DataType] = {
    "player_id": pl.Int64,
    "through_date": pl.Date,
    "season": pl.Utf8,
    "player_name": pl.Utf8,
    "mins": pl.Float64,
    "lebron": pl.Float64,
    "o_lebron": pl.Float64,
    "d_lebron": pl.Float64,
    "war": pl.Float64,
}


def load_lebron_season(path: str) -> pl.DataFrame:
    """Parse a LEBRON season-file CSV (e.g. ``lebron-data-2026.csv``).

    ``seasons`` is passed through as a raw string -- per-season files carry a
    single year (``"2026"``); the combined all-years file carries a
    multi-year window (``"2010-2013"``). Both parse with this one function.

    Args:
        path: Filesystem path to a LEBRON season CSV.

    Returns:
        Frame with schema :data:`LEBRON_SEASON_ORACLE_SCHEMA`. Zero rows
        (with that schema) when the file has a header but no data rows.

    Raises:
        FileNotFoundError: If ``path`` does not exist.

    Example:
        Load the current-season LEBRON file::

            from sportsdataverse.nba.nba_oracle_data import load_lebron_season
            oracle = load_lebron_season(f"{oracle_dir}/lebron-data-2026.csv")
    """
    raw = pl.read_csv(path)
    if raw.is_empty():
        return pl.DataFrame(schema=LEBRON_SEASON_ORACLE_SCHEMA)
    return raw.select(
        pl.col("nba_id").cast(pl.Int64).alias("player_id"),
        pl.col("Player").cast(pl.Utf8).alias("player_name"),
        pl.col("Seasons").cast(pl.Utf8).alias("seasons"),
        pl.col("Team").cast(pl.Utf8).alias("team"),
        pl.col("LEBRON").cast(pl.Float64).alias("lebron"),
        pl.col("O-LEBRON").cast(pl.Float64).alias("o_lebron"),
        pl.col("D-LEBRON").cast(pl.Float64).alias("d_lebron"),
        pl.col("WAR").cast(pl.Float64).alias("war"),
    )


def load_lebron_daily(path: str) -> pl.DataFrame:
    """Parse a LEBRON daily-snapshot CSV (e.g. ``lebron_daily_2026-07-02.csv``).

    Args:
        path: Filesystem path to a LEBRON daily-snapshot CSV.

    Returns:
        Frame with schema :data:`LEBRON_DAILY_ORACLE_SCHEMA`. Zero rows
        (with that schema) when the file has a header but no data rows.

    Raises:
        FileNotFoundError: If ``path`` does not exist.

    Example:
        Load the most recent daily snapshot (glob for the dated filename)::

            import glob
            from sportsdataverse.nba.nba_oracle_data import load_lebron_daily
            latest = sorted(glob.glob(f"{oracle_dir}/lebron_daily_*.csv"))[-1]
            oracle = load_lebron_daily(latest)
    """
    raw = pl.read_csv(path)
    if raw.is_empty():
        return pl.DataFrame(schema=LEBRON_DAILY_ORACLE_SCHEMA)
    return raw.select(
        pl.col("PLAYER_ID").cast(pl.Int64).alias("player_id"),
        pl.col("ThroughDate").cast(pl.Utf8).str.to_date("%Y-%m-%d").alias("through_date"),
        pl.col("Season").cast(pl.Utf8).alias("season"),
        pl.col("Name").cast(pl.Utf8).alias("player_name"),
        pl.col("Mins").cast(pl.Float64).alias("mins"),
        pl.col("LEBRON").cast(pl.Float64).alias("lebron"),
        pl.col("OLEBRON").cast(pl.Float64).alias("o_lebron"),
        pl.col("DLEBRON").cast(pl.Float64).alias("d_lebron"),
        pl.col("LEBRON WAR").cast(pl.Float64).alias("war"),
    )


#: Tidy schema for :func:`load_darko_dpm`.
DARKO_DPM_ORACLE_SCHEMA: dict[str, pl.DataType] = {
    "player_name": pl.Utf8,
    "team": pl.Utf8,
    "dpm": pl.Int64,
    "odpm": pl.Int64,
    "ddpm": pl.Int64,
}


def _signed_int(s: str) -> int:
    """Parse a sign-prefixed integer string (``"+7"`` / ``"-2"``) -> int."""
    return int(s.replace("+", "").strip())


def load_darko_dpm(path: str) -> pl.DataFrame:
    """Parse a DARKO DPM leaderboard CSV (e.g. ``2026-darko-dpm-leaderboard.csv``).

    Name-keyed only (no shared player id with the model zoo) -- this is the
    family :func:`~sportsdataverse.nba.nba_model_validation.external_validity`
    joins with ``join="name"``. Handles two real-file quirks: a leading UTF-8
    BOM (read with ``encoding="utf-8-sig"``, which strips it) and
    sign-prefixed integer columns (``"+7"``, not ``"7"``).

    Args:
        path: Filesystem path to a DARKO DPM leaderboard CSV.

    Returns:
        Frame with schema :data:`DARKO_DPM_ORACLE_SCHEMA`. Zero rows (with
        that schema) when the file has a header but no data rows.

    Raises:
        FileNotFoundError: If ``path`` does not exist.

    Example:
        Load the leaderboard and inspect the DPM column::

            from sportsdataverse.nba.nba_oracle_data import load_darko_dpm
            oracle = load_darko_dpm(f"{oracle_dir}/2026-darko-dpm-leaderboard.csv")
            print(oracle.sort("dpm", descending=True).head())
    """
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        return pl.DataFrame(schema=DARKO_DPM_ORACLE_SCHEMA)
    return pl.DataFrame(
        {
            "player_name": [r["Player"] for r in rows],
            "team": [r["Team"] for r in rows],
            "dpm": [_signed_int(r["DPM"]) for r in rows],
            "odpm": [_signed_int(r["ODPM"]) for r in rows],
            "ddpm": [_signed_int(r["DDPM"]) for r in rows],
        },
        schema=DARKO_DPM_ORACLE_SCHEMA,
    )
