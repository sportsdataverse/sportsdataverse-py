"""`fill_null(0.0)` is a silent no-op on Boolean columns -- guard the fix.

polars leaves boolean nulls untouched when the fill value is a float. No error,
no warning. Any `.mean()` afterwards then averages the non-null rows only, and
for a flag that is null-where-absent that means it averages over exactly the True
rows and returns 1.0.

`rushing_power_rate` shipped that way in every published season: in 2024
`power_rush_attempt` is null on 159,513 plays and True on 3,437, so the rate read
1.0 for every team instead of 3437/63017 = 0.055.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.cfb.cfb_pbp import _fill_missing


def test_float_fill_really_is_a_noop_on_booleans() -> None:
    """Pin the polars behaviour the helper exists to work around.

    If this ever starts failing, polars changed and the helper can be revisited --
    but silently keeping the workaround would then hide a real regression.
    """
    df = pl.DataFrame({"flag": [True, None, None]})
    assert df.fill_null(0.0)["flag"].to_list() == [True, None, None]
    assert df.fill_null(0.0)["flag"].mean() == 1.0  # the bug, exactly


def test_helper_fills_booleans_as_false() -> None:
    df = pl.DataFrame({"flag": [True, None, None]})
    out = _fill_missing(df)
    assert out["flag"].to_list() == [True, False, False]
    assert out["flag"].mean() == 1 / 3


def test_helper_still_fills_numerics_with_zero() -> None:
    df = pl.DataFrame({"yards": [1.0, None, 3.0], "n": [1, None, 3]})
    out = _fill_missing(df)
    assert out["yards"].to_list() == [1.0, 0.0, 3.0]
    assert out["n"].to_list() == [1, 0, 3]


def test_helper_is_a_noop_when_nothing_is_null() -> None:
    """The 42 aggregations whose source column has no nulls must not move."""
    df = pl.DataFrame({"flag": [True, False, True], "yards": [1.0, 2.0, 3.0]})
    out = _fill_missing(df)
    assert out.equals(df)


def test_rate_over_a_null_where_absent_flag_is_the_real_denominator() -> None:
    """The shape of the actual defect: 2 power attempts out of 6 rushes."""
    rushes = pl.DataFrame({"power_rush_attempt": [True, None, None, True, None, None]})
    assert rushes["power_rush_attempt"].mean() == 1.0  # before
    assert _fill_missing(rushes)["power_rush_attempt"].mean() == 2 / 6  # after
