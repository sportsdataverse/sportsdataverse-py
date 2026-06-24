"""Regression tests for the ID-column-type + name-matching conventions in CLAUDE.md.

These pin the exact bug classes that historically only surfaced downstream (after a
model rebuild or a join produced wrong/empty matches): the ``id -> Utf8`` "paper-over"
cast on a float-origin id, join-key dtype disagreement, and case-sensitive player-name
regexes. All offline (polars + static dicts + regex) — no live API, so they run in CI
and in ``/preflight`` on every change.

See the "ID column types (join keys / player & team IDs)" section of CLAUDE.md.
"""

from __future__ import annotations

import polars as pl
import pytest


def test_float_origin_id_naive_utf8_is_the_trap() -> None:
    """A float-origin id stringifies as ``"123.0"`` — the documented foot-gun.

    JSON with nulls in an id column yields Float64; a naive ``cast(Utf8)`` then
    produces ``"123.0"``, silently breaking a join against ``"123"``. The convention
    is to cast the *raw integer* first.
    """
    df = pl.DataFrame({"id": [123.0]})  # Float64 (e.g. a nullable id column from JSON)

    naive = df.select(pl.col("id").cast(pl.Utf8)).item()
    assert naive == "123.0", "polars float->Utf8 still adds the decimal; convention still needed"

    safe = df.select(pl.col("id").cast(pl.Int64).cast(pl.Utf8)).item()
    assert safe == "123", "cast Int64 then Utf8 is the documented safe path"


def test_id_join_requires_dtype_agreement() -> None:
    """A join on an id with disagreeing dtypes fails loudly; aligning the key fixes it.

    This is why CLAUDE.md says to assert ``left.schema[key] == right.schema[key]`` before
    a join — the failure mode the convention prevents is wrong/empty matches at test time.
    """
    left = pl.DataFrame({"player_id": [1, 2], "x": [10, 20]})  # Int64 key
    right = pl.DataFrame({"player_id": ["1", "2"], "name": ["a", "b"]})  # Utf8 key

    with pytest.raises(Exception):  # noqa: B017 - polars raises a SchemaError on dtype mismatch
        left.join(right, on="player_id", how="left")

    aligned = left.with_columns(pl.col("player_id").cast(pl.Utf8)).join(right, on="player_id", how="left")
    assert aligned.height == 2
    assert aligned["name"].to_list() == ["a", "b"]


def test_cfb_name_extraction_regex_is_case_insensitive() -> None:
    """The documented inline-case-toggle pattern folds the prefix case but keeps the
    captured proper-noun case-sensitive (so lowercase narrative tails aren't captured).

    Mirrors the CLAUDE.md "sacked by" example; polars/Rust regex has no lookaround, so
    the ``(?i)prefix(?-i: NAMES)`` toggle is the load-bearing idiom this guards.
    """
    pat = r"(?i)sacked by(?-i: ([A-Z][\w'\.\-]+(?:\s+[A-Z][\w'\.\-]+)?))"
    s = pl.Series(
        [
            "Smith pass SACKED BY John Doe for -7 yards",  # uppercase prefix
            "sacked by Jane Roe at the 20",  # lowercase prefix
            "no sack on the play",  # no match
        ]
    )
    out = s.str.extract(pat, 1).to_list()
    assert out == ["John Doe", "Jane Roe", None]


def test_nfl_static_mappings_are_str_keyed() -> None:
    """The bundled NFL crosswalk dicts must be str->str — an accidental int key/value
    would silently fail to match the string ids/names they're joined against."""
    from sportsdataverse.nfl import player_name_mapping, team_abbr_mapping

    for name, mapping in (("team_abbr_mapping", team_abbr_mapping), ("player_name_mapping", player_name_mapping)):
        assert mapping, f"{name} is unexpectedly empty"
        bad = [(k, v) for k, v in mapping.items() if not isinstance(k, str) or not isinstance(v, str)]
        assert not bad, f"{name} has non-str entries: {bad[:5]}"
