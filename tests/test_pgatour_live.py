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
        ("schedule_years", {"tour_code": "R"}, 10),
        ("tournaments", {"tournament_ids": ["R2026014", "R2026006"]}, 2),
        ("past_champions", {"tournament_id": "R2026014"}, 80),
        ("hole_by_hole", {"tournament_id": "R2026014", "round": 4}, 900),
        ("leaderboard_stats", {"tournament_id": "R2026014"}, 250),
        ("field", {"tournament_id": "R2026014"}, 80),
        ("past_results", {"tournament_id": "R2026014", "year": 2025}, 80),
        ("course_stats", {"tournament_id": "R2026014"}, 90),
        ("hole_details", {"tournament_id": "R2026014", "course_id": "014", "hole": 12}, 1),
        ("scorecard_stats", {"tournament_id": "R2026014", "player_id": "28237"}, 15),
        ("player_results", {"player_id": "28237"}, 300),
    ],
)
def test_pgatour_graphql_live(short, kwargs, min_rows):
    from sportsdataverse.golf import pgatour

    from sportsdataverse.golf import pgatour_parsers

    df = getattr(pgatour, f"pgatour_{short}")(**kwargs)
    assert df.height >= min_rows, f"pgatour_{short}: {df.height} rows"
    assert df.schema == getattr(pgatour_parsers, f"parse_pgatour_{short}")(None).schema
    id_cols = [c for c in ("tournament_id", "player_id", "stat_id") if c in df.columns]
    assert df.select(id_cols).null_count().row(0) == (0,) * len(id_cols), "an id column came back null"


def test_pgatour_past_results_four_digit_year_is_that_edition():
    # the API alone answers the CURRENT edition for year=2025; the wrapper sends 20250
    from sportsdataverse.golf import pgatour_past_results

    df = pgatour_past_results(tournament_id="R2026014", year=2025)
    assert df["tournament_id"].unique().to_list() == ["R2025014"]
