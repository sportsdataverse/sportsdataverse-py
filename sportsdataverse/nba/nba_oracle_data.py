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

import re
import unicodedata

import polars as pl

__all__ = [
    "normalize_player_name",
    "RAPM_ORACLE_SCHEMA",
    "load_rapm_ryan_davis",
    "EPM_ORACLE_SCHEMA",
    "load_epm",
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
