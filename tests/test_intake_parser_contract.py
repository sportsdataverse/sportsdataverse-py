"""The intake-family parsers never raise, including on source shapes no capture happens to hold.

Every one of these parsers promises ``Raises: None`` and a zero-row frame on anything unusable.
Three real source shapes broke that promise before this file existed, and none of them appears in
a committed capture -- so only a test like this one keeps them fixed:

* **one key typed inconsistently across records** (``[{"id": 7}, {"id": "8"}]``). The ledger
  records OpenLigaDB serializing ``leagueSeason`` as both ``"2007"`` and ``2025``, so this is a
  shape these providers really do produce. ``pl.from_pandas`` raised ``ArrowInvalid``.
* **a list in one record and a scalar in another** (``[{"x": [1, 2]}, {"x": 5}]``) -- ``TypeError``.
* **two keys that snake_case to the same column** (``{"HomeTeam": 1, "homeTeam": 2}``) --
  ``AttributeError``, because the duplicated name makes ``pdf[name]`` a DataFrame.

A missing key is NOT inconsistent typing: ``json_normalize`` fills it with ``NaN``, and treating
that as a second type would stringify every boolean column that is absent from one record (it did,
on four real Polymarket columns, until the null guard went in).
"""

import importlib

import polars as pl
import pytest

PARSERS = [
    ("sportsdataverse.espn_content.espn_content_parsers", "parse_espn_content"),
    ("sportsdataverse.thesportsdb.thesportsdb_parsers", "parse_thesportsdb"),
    ("sportsdataverse.soccer.openligadb_parsers", "parse_openligadb"),
    ("sportsdataverse.odds.polymarket_parsers", "parse_polymarket"),
    ("sportsdataverse.odds.kalshi_parsers", "parse_kalshi"),
]

HOSTILE = [
    pytest.param([{"id": 7}, {"id": "8"}], id="one-key-two-types"),
    pytest.param([{"x": [1, 2]}, {"x": 5}], id="list-and-scalar"),
    pytest.param([{"x": {"a": 1}}, {"x": 5}], id="dict-and-scalar"),
    pytest.param([{"HomeTeam": 1, "homeTeam": 2}], id="names-collide-when-snake-cased"),
]


def _load(mod, fn):
    return getattr(importlib.import_module(mod), fn)


@pytest.mark.parametrize("mod,fn", PARSERS)
@pytest.mark.parametrize("payload", HOSTILE)
def test_a_heterogeneous_body_yields_a_frame_not_an_exception(mod, fn, payload):
    df = _load(mod, fn)(payload)
    assert isinstance(df, pl.DataFrame)
    assert df.height == len(payload)


@pytest.mark.parametrize("mod,fn", PARSERS)
@pytest.mark.parametrize("payload", [None, {}, [], "", 7, [1, 2, 3]])
def test_an_unusable_body_is_a_zero_row_frame(mod, fn, payload):
    parser = _load(mod, fn)
    if fn == "parse_openligadb" and isinstance(payload, (str, int)) and str(payload).strip():
        # openligadb's /getlastchangedate really does answer a bare scalar, which is one row.
        pytest.skip("a bare scalar is a documented one-row body for this family")
    df = parser(payload)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


@pytest.mark.parametrize("mod,fn", PARSERS)
def test_a_missing_key_does_not_restringify_a_boolean_column(mod, fn):
    """A key absent from one record is NaN-filled, not a second type."""
    df = _load(mod, fn)([{"flag": True, "n": 1}, {"n": 2}])
    assert df.schema["flag"] == pl.Boolean, dict(df.schema)
