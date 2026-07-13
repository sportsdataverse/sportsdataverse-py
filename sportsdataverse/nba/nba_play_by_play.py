"""ESPN NBA play-by-play producer -- polars port of ``hoopR:::helper_espn_nba_pbp``.

Source: ``hoopR/R/espn_nba_data.R`` lines 2873-3280. The NBA helper is
semantically identical to the WNBA one (``wnba_play_by_play.py`` /
``wbb_play_by_play.py``) after league normalization -- same statement count,
same free-throw coordinate pin, same three participant-athlete columns
(``athlete_id_1/2/3``), same payload-first-seen column union with no terminal
select.

NBA shares the **WNBA clock contract, not WBB's**: NBA game clocks tick in
fractions of a second in the final minute, so ``clock.seconds`` and the six
derived ``*_seconds_remaining`` features are doubles in the payload and Float64
in the release (verified: 33/400 sampled clock values non-integer). The WNBA
``_INT32_COLS`` / ``_FLOAT64_COLS`` tuples (which move those seven columns to
Float64) are therefore the correct NBA tuples and are imported verbatim.

The R-released ``espn_nba_pbp`` parquet is the parity oracle, with the same ONE
deliberate dtype improvement as WBB/WNBA (#245): the play ``id`` is emitted
Int64 rather than R's precision-losing Float64. NBA ``id`` values (~12 digits)
exceed Int32 but sit under 2^53, so the released Float64 holds them exactly and
the ``dtype_upgrades={"id": (pl.Int64(), pl.Float64())}`` parity escape hatch
compares equal after the cast. ``id`` is read straight from the payload (a
handful of rows carry ``id != concat(game_id, sequence_number)``), never
reconstructed.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.wbb.wbb_play_by_play import _basketball_play_by_play
from sportsdataverse.wnba.wnba_play_by_play import (
    _FLOAT64_COLS as _FLOAT64_COLS,
)
from sportsdataverse.wnba.wnba_play_by_play import (
    _INT32_COLS as _INT32_COLS,
)

__all__ = ["helper_nba_play_by_play"]


def helper_nba_play_by_play(final: dict) -> pl.DataFrame:
    """Parse one game's stored payload into the released play-by-play frame.

    Faithful polars port of ``hoopR:::helper_espn_nba_pbp``
    (``hoopR/R/espn_nba_data.R:2873``). Returns one row per play whose column
    set, order, and dtypes match the R-released ``espn_nba_pbp`` parquet for
    that game's season (NBA-only payload columns such as ``qtr`` / ``game_half``
    / ``athlete_id_3`` / ``type_abbreviation`` surface via the payload-first-seen
    column union). The seven clock columns are Float64 (WNBA contract), and the
    play ``id`` is Int64 (read from the payload).

    Args:
        final: One game's stored payload (the ``final.json`` the
            ``hoopR-nba-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: One row per play. Empty (zero-column) frame when the
        payload has ``playByPlaySource == "none"`` or 10 or fewer plays
        (R guard) -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.nba import helper_nba_play_by_play
            final = json.load(open("401766128.json", encoding="utf-8"))
            df = helper_nba_play_by_play(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("shooting_play") == True).height

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _basketball_play_by_play(final, int32_cols=_INT32_COLS, float64_cols=_FLOAT64_COLS)
