"""ESPN MBB play-by-play producer -- polars port of ``hoopR:::helper_espn_mbb_pbp``.

Source: ``hoopR/R/espn_mbb_data.R`` lines 2951-3235. The MBB helper is
semantically identical to the WBB one (``wbb_play_by_play.py``) after league
normalization (token-ratio 0.9966; the only residual diffs are the function
name and one pipe-style-vs-nested ``jsonlite::toJSON`` call) -- same free-throw
coordinate pin (dead for MBB: ``type.text`` ships ``"MadeFreeThrow"`` with no
space, so R's ``str_detect(., "Free Throw")`` never matches), same two
participant-athlete columns (``athlete_id_1/2`` -- MBB never populates a third
slot), same both-teams-agnostic column union.

**MBB shares the WBB clock contract, NOT NBA/WNBA's.** MBB game clocks are
whole-integer seconds in every sampled payload (2022 and 2025 vintages), so
``clock_seconds`` and the ``*_seconds_remaining`` columns stay Int32 -- unlike
NBA/WNBA, where sub-second clocks force Float64. MBB does NOT import the WNBA
``_FRACTIONAL_CLOCK_COLS`` override.

**The one real delta: a halves-reshaping vintage split.** MBB payloads from
2023+ ship ``half``/``lag_period``/``lead_period``/``start.period_seconds_
remaining``/``end.period_seconds_remaining`` instead of the WBB/WNBA
``qtr``/``game_half``/``lag_qtr``/``lead_qtr``/``start.quarter_seconds_
remaining`` shape (pre-2023 MBB payloads still use the quarters shape, which is
already covered by WBB's ``_INT32_COLS``). The four period-suffixed columns
have no WBB/WNBA counterpart in any released season, so they're appended to
WBB's int32 tuple here. The payload-first-seen column union (the core's
generic mechanism) handles the vintage split with no per-era branching --
whichever key set a given game's payload carries is what gets unioned in.

The R-released ``espn_mens_college_basketball_pbp`` parquet is the parity
oracle, with the same ONE deliberate dtype improvement as WBB/WNBA/NBA (#245):
the play ``id`` is emitted Int64 rather than R's precision-losing Float64. MBB
``id`` is an 18-digit concatenation that overflows R's double mantissa (2^53)
-- the released Float64 has ~906k colliding ids in the 2025 season alone
(~41% of rows), so reading ``id`` straight from the payload as Int64 is the
deliberate improvement here (same precedent as #245), and parity is asserted
through the oracle's lossy Float64 view (cast-up, not cast-down).
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.wbb.wbb_play_by_play import (
    _FLOAT64_COLS as _FLOAT64_COLS,
)
from sportsdataverse.wbb.wbb_play_by_play import (
    _INT32_COLS as _WBB_INT32_COLS,
)
from sportsdataverse.wbb.wbb_play_by_play import _basketball_play_by_play

__all__ = ["helper_mbb_play_by_play"]

# WBB's Int32 tuple plus the four halves-reshaping (2023+ vintage) columns that
# have no WBB/WNBA counterpart -- see the module docstring.
_INT32_COLS: tuple[str, ...] = _WBB_INT32_COLS + (
    "start_period_seconds_remaining",
    "end_period_seconds_remaining",
    "lag_period",
    "lead_period",
)


def helper_mbb_play_by_play(final: dict) -> pl.DataFrame:
    """Parse one game's stored payload into the released play-by-play frame.

    Faithful polars port of ``hoopR:::helper_espn_mbb_pbp``
    (``hoopR/R/espn_mbb_data.R:2951``). Returns one row per play whose column
    set, order, and dtypes match the R-released ``espn_mens_college_
    basketball_pbp`` parquet for that game's season (MBB-only payload columns
    such as ``short_description`` / ``lag_game_half`` / ``lead_game_half``
    surface via the payload-first-seen column union). The clock columns stay
    Int32 (WBB contract, not WNBA/NBA's Float64), and the play ``id`` is Int64
    (read from the payload).

    Args:
        final: One game's stored payload (the ``final.json`` the
            ``hoopR-mbb-raw`` scraper persists) as a dict.

    Returns:
        pl.DataFrame: One row per play. Empty (zero-column) frame when the
        payload has ``playByPlaySource == "none"`` or 10 or fewer plays
        (R guard) -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.mbb import helper_mbb_play_by_play
            final = json.load(open("401746082.json", encoding="utf-8"))
            df = helper_mbb_play_by_play(final)
            print(df.shape)

        Pipeline next step (one line)::

            df.filter(pl.col("shooting_play") == True).height

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return _basketball_play_by_play(final, int32_cols=_INT32_COLS, float64_cols=_FLOAT64_COLS)
