"""ESPN WNBA play-by-play producer -- polars port of ``wehoop:::helper_espn_wnba_pbp``.

Source: ``wehoop/R/espn_wnba_data.R`` lines 2056-2337 (wehoop 3.0.0). The WNBA
helper is semantically identical to the WBB one (``wbb_play_by_play.py``)
except for two league facts:

* WNBA names a THIRD participant athlete column (``athlete_id_3``; WNBA
  payloads carry ``participants.2.athlete.id``) -- the shared core already
  emits it for both leagues.
* WNBA game clocks tick in fractions of a second in the final minute, so R's
  jsonlite reads ``clock.seconds`` (and the six derived
  ``*_seconds_remaining`` features) as doubles and the release keeps them
  Float64, where WBB releases them Int32.

The R-released ``espn_wnba_pbp`` parquet is the parity oracle, with the same
ONE deliberate dtype improvement as WBB (#245): the play ``id`` is emitted
Int64 rather than R's precision-losing Float64.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.wbb.wbb_play_by_play import (
    _FLOAT64_COLS as _WBB_FLOAT64_COLS,
)
from sportsdataverse.wbb.wbb_play_by_play import (
    _INT32_COLS as _WBB_INT32_COLS,
)
from sportsdataverse.wbb.wbb_play_by_play import (
    _basketball_play_by_play,
)

# WNBA clocks are sub-second (tenths in the final minute), so these seven
# columns are fractional in the payload and Float64 in the release.
_FRACTIONAL_CLOCK_COLS: tuple[str, ...] = (
    "clock_seconds",
    "start_quarter_seconds_remaining",
    "start_half_seconds_remaining",
    "start_game_seconds_remaining",
    "end_quarter_seconds_remaining",
    "end_half_seconds_remaining",
    "end_game_seconds_remaining",
)
_INT32_COLS: tuple[str, ...] = tuple(c for c in _WBB_INT32_COLS if c not in _FRACTIONAL_CLOCK_COLS)
_FLOAT64_COLS: tuple[str, ...] = _WBB_FLOAT64_COLS + _FRACTIONAL_CLOCK_COLS

__all__ = ["helper_wnba_play_by_play"]


def helper_wnba_play_by_play(final: dict) -> pl.DataFrame:
    """Parse one game's stored payload into the released play-by-play frame.

    Faithful polars port of ``wehoop:::helper_espn_wnba_pbp``
    (``wehoop/R/espn_wnba_data.R:2056``). Returns one row per play whose
    column set, order, and dtypes match the R-released ``espn_wnba_pbp``
    parquet for that game's season (WNBA-only payload columns such as
    ``points_attempted`` / ``short_description`` / ``athlete_id_3`` surface
    via the payload-first-seen column union).

    Args:
        final: One game's stored payload (the ``final.json`` the
            ``wehoop-wnba-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: One row per play. Empty (zero-column) frame when the
        payload has ``playByPlaySource == "none"`` or 10 or fewer plays
        (R guard) -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba import helper_wnba_play_by_play
            final = json.load(open("401736126.json", encoding="utf-8"))
            df = helper_wnba_play_by_play(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("shooting_play") == True).height

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return _basketball_play_by_play(final, int32_cols=_INT32_COLS, float64_cols=_FLOAT64_COLS)
