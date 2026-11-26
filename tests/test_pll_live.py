"""Live Premier Lacrosse League stats tests (``pll_*``), gated by ``SDV_PY_LIVE_TESTS=1``."""

from __future__ import annotations

import pytest

from tests.conftest import skip_if_no_live

pytestmark = skip_if_no_live


@pytest.mark.parametrize(
    "short, kwargs, min_rows",
    [
        ("schedule", {"year": 2026}, 50),
        ("game", {"event_slug": "2026-ev-4"}, 1),
        ("play_by_play", {"event_slug": "2026-ev-4"}, 150),
        ("player_box", {"event_slug": "2026-ev-4"}, 30),
        ("team_box", {"event_slug": "2026-ev-4"}, 2),
        ("game_insights", {"event_slug": "2026-ev-4"}, 1),
        ("standings", {"year": 2026}, 8),
        ("team_season_stats", {"year": 2026}, 8),
        ("team_stats", {"team_slug": "waterdogs", "year": 2026}, 1),
        ("team_roster", {"team_slug": "waterdogs", "year": 2026}, 20),
        ("player_season_stats", {"year": 2026}, 200),
        ("player_advanced_stats", {"year": 2026}, 150),
        ("season_leaders", {"year": 2026, "limit": 5, "stats": ["points", "assists"]}, 10),
        ("career_leaders", {"stats": ["points", "goals"], "limit": 5}, 10),
        ("player", {"player_slug": "michael-sowers"}, 1),
        ("player_seasons", {"player_slug": "michael-sowers"}, 5),
        ("glg_schedule", {"glg_league": "intlm", "year": 2026}, 4),
        ("glg_standings", {"glg_league": "intlw", "year": 2026}, 3),
        ("wll_schedule", {"year": 2026}, 5),
    ],
)
def test_pll_live(short, kwargs, min_rows):
    from sportsdataverse.lacrosse import pll

    df = getattr(pll, f"pll_{short}")(**kwargs)
    assert df.height >= min_rows, f"pll_{short}: {df.height} rows"
