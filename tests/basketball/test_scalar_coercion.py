"""Scalar coercions in the basketball parsers: whole floats are ids, never ``"123.0"``; NaN / inf never raise."""

from __future__ import annotations

import subprocess
import sys

import pytest

from sportsdataverse.basketball.acb import acb_parsers as acb
from sportsdataverse.basketball.atrium import atrium_parsers as atrium
from sportsdataverse.basketball.kbl import kbl_parsers as kbl
from sportsdataverse.basketball.tbf import tbf_parsers as tbf

STRINGIFIERS = [acb._s, atrium._text, tbf._s]


@pytest.mark.parametrize("fn", STRINGIFIERS, ids=["acb._s", "atrium._text", "tbf._s"])
def test_a_whole_float_id_is_its_digits(fn):
    assert fn(123.0) == "123"
    assert fn(-7.0) == "-7"
    assert fn(123) == "123"
    assert fn(1.5) == "1.5"


@pytest.mark.parametrize("fn", STRINGIFIERS, ids=["acb._s", "atrium._text", "tbf._s"])
@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_a_non_finite_float_is_null_not_an_error(fn, bad):
    assert fn(bad) is None


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), 1.5])
def test_kbl_int_refuses_non_integral_and_non_finite_floats_quietly(bad):
    assert kbl._i(bad) is None


def test_kbl_int_keeps_whole_floats():
    assert kbl._i(12.0) == 12 and kbl._i(" 12 ") == 12 and kbl._i(True) is None


@pytest.mark.parametrize("bad", [float("nan"), float("inf")])
def test_acb_int_is_quiet_on_non_finite_floats(bad):
    assert acb._i(bad) is None


def test_tbf_table_check_survives_python_O():
    """The ROWS / TABLES consistency check is an explicit raise, so ``python -O`` still runs it (and passes)."""
    out = subprocess.run(
        [
            sys.executable,
            "-O",
            "-c",
            "import sportsdataverse.basketball.tbf.tbf_parsers as p; print(set(p.ROWS) == set(p.TABLES))",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert out.stdout.strip() == "True"
