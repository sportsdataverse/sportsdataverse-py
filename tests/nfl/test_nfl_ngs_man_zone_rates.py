"""Unit tests for nfl_ngs_man_zone_rates (offline, synthetic loader)."""

import polars as pl

from sportsdataverse.nfl.nfl_ngs_tracking import _MAN_ZONE_SCHEMA, nfl_ngs_man_zone_rates


def _fake_loader(seasons, return_as_pandas=False):
    # 6 MAN + 4 ZONE plays against KC's defense (BUF has the ball in KC's home
    # game -> defteam KC); one unlabelled play that must be dropped.
    labels = ["MAN_COVERAGE"] * 6 + ["ZONE_COVERAGE"] * 4 + [None]
    coverage = ["COVER_1"] * 6 + ["COVER_3"] * 3 + ["2_MAN"] + [None]
    return pl.DataFrame(
        {
            "nflverse_game_id": ["2023_01_BUF_KC"] * 11,
            "play_id": list(range(1, 12)),
            "possession_team": ["BUF"] * 11,
            "defense_man_zone_type": labels,
            "defense_coverage_type": coverage,
        }
    )


def test_rates_and_denominator():
    out = nfl_ngs_man_zone_rates([2023], _loader=_fake_loader)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["season"] == 2023 and row["defteam"] == "KC"
    assert row["plays"] == 10  # unlabelled play dropped
    assert abs(row["man_rate"] - 0.6) < 1e-9
    assert abs(row["zone_rate"] - 0.4) < 1e-9
    assert abs(row["man_rate"] + row["zone_rate"] - 1.0) < 1e-9
    assert abs(row["cover_1_rate"] - 0.6) < 1e-9
    assert abs(row["cover_3_rate"] - 0.3) < 1e-9
    assert row["cover_0_rate"] == 0.0


def test_defteam_flips_with_possession():
    def _loader(seasons, return_as_pandas=False):
        return pl.DataFrame(
            {
                "nflverse_game_id": ["2023_01_BUF_KC"] * 2,
                "play_id": [1, 2],
                "possession_team": ["KC", "KC"],
                "defense_man_zone_type": ["MAN_COVERAGE", "ZONE_COVERAGE"],
                "defense_coverage_type": ["COVER_0", "COVER_2"],
            }
        )

    out = nfl_ngs_man_zone_rates([2023], _loader=_loader)
    assert out["defteam"].to_list() == ["BUF"]


def test_uncharted_season_returns_schema_frame():
    def _all_null(seasons, return_as_pandas=False):
        return pl.DataFrame(
            {
                "nflverse_game_id": ["2024_01_BUF_KC"],
                "play_id": [1],
                "possession_team": ["BUF"],
                "defense_man_zone_type": [None],
                "defense_coverage_type": [None],
            }
        )

    out = nfl_ngs_man_zone_rates([2024], _loader=_all_null)
    assert out.height == 0
    assert dict(out.schema) == _MAN_ZONE_SCHEMA
