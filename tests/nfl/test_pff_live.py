"""Live-gated smoke tests for the PFF Premium Stats wrappers.

Gated by ``@skip_if_no_pff_live`` (env ``SDV_PY_PFF_LIVE=1``) -- NOT the generic
``SDV_PY_LIVE_TESTS``. PFF Premium is paywalled + residential, so no CI workflow sets this
gate; run it only from a logged-in residential session with the ``SDV_PY_PFF_PREMIUM_KEY``
(+ optional ``SDV_PY_PFF_SESSION``) cookies exported.
"""

import polars as pl

from sportsdataverse.nfl import pff as nflpff
from tests.conftest import skip_if_no_pff_live

pytestmark = skip_if_no_pff_live


def test_live_leagues():
    df = nflpff.pff_leagues()
    assert isinstance(df, pl.DataFrame) and df.height > 0


def test_live_facet_passing_summary():
    df = nflpff.pff_facet_passing_summary(season=2024)
    assert isinstance(df, pl.DataFrame)
    assert "player_id" in df.columns


def test_live_player_detail():
    df = nflpff.pff_player_passing_summary(player_id=11765, season=2024)
    assert isinstance(df, pl.DataFrame)
    assert "game_id" in df.columns
