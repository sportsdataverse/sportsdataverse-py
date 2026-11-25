"""Live PGA TOUR GraphQL tests (``pgatour_*``), gated by ``SDV_PY_LIVE_TESTS=1``."""

from __future__ import annotations

import pytest

from tests.conftest import skip_if_no_live

pytestmark = skip_if_no_live


@pytest.mark.parametrize(
    "short, kwargs, min_rows",
    [
        ("schedule", {"tour_code": "R", "year": 2026}, 40),
        ("leaderboard", {"tournament_id": "R2026014"}, 80),
        ("scorecard", {"tournament_id": "R2026014", "player_id": "28237"}, 72),
        ("tee_times", {"tournament_id": "R2026014"}, 200),
        ("player", {"player_id": "28237"}, 1),
        ("player_directory", {"tour_code": "R", "active": True}, 150),
        ("stat_catalog", {"tour_code": "R", "year": 2026}, 200),
        ("stat_details", {"stat_id": "120", "tour_code": "R", "year": 2026}, 300),
        ("tour_cup", {"tour_code": "R", "year": 2025}, 150),
        ("shot_details", {"tournament_id": "R2026006", "player_id": "59095", "round": 4}, 50),
    ],
)
def test_pgatour_graphql_live(short, kwargs, min_rows):
    from sportsdataverse.golf import pgatour

    df = getattr(pgatour, f"pgatour_{short}")(**kwargs)
    assert df.height >= min_rows, f"pgatour_{short}: {df.height} rows"
