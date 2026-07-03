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

__all__ = ["normalize_player_name"]


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
