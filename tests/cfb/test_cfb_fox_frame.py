"""Offline contract test for the Fox empty-frame schema.

Deliberately a separate module: ``tests/cfb/test_cfb_fox.py`` sets
``pytestmark = skip_if_no_live``, so an offline test placed there would be
skipped along with the live suite and would guard nothing.
"""

from __future__ import annotations

from sportsdataverse.cfb.cfb_fox_ext import _TEAM_GAMELOG_COLUMNS, _frame


def test_empty_frame_carries_the_documented_schema():
    """An empty response must still name its columns, in both backends.

    Zero-COLUMN empties are why an upstream-empty Fox response was
    indistinguishable from a broken parser -- a caller inspecting ``df.columns``
    learned nothing either way.
    """
    empty = _frame([], False, _TEAM_GAMELOG_COLUMNS)
    assert empty.height == 0
    assert list(empty.columns) == list(_TEAM_GAMELOG_COLUMNS)

    empty_pd = _frame([], True, _TEAM_GAMELOG_COLUMNS)
    assert len(empty_pd) == 0
    assert list(empty_pd.columns) == list(_TEAM_GAMELOG_COLUMNS)


def test_the_schema_argument_does_not_disturb_a_populated_frame():
    rows = [{"team_id": "11", "season_type": "REG", "category": "passing"}]
    assert list(_frame(rows, False, _TEAM_GAMELOG_COLUMNS).columns) == ["team_id", "season_type", "category"]


def test_no_schema_keeps_the_old_behaviour():
    """Callers that pass no schema (league_leaders, whose columns are
    response-shaped) must be unchanged."""
    assert _frame([], False).width == 0
