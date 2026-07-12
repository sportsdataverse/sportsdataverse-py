"""Byte-for-byte migration gate (T7.2): every per-sport metric symbol that was
routed through ``_common.metrics`` must BE the shared object (redundant-alias
re-export), guaranteeing identical behavior to the pre-refactor inline copy.
"""

from __future__ import annotations

import importlib

import pytest

import sportsdataverse._common.metrics as core

# module -> metric names re-exported from _common.metrics (mirrors the migration)
MIGRATED = {
    "sportsdataverse.cfb.cfb_prediction_constants": [
        "brier_score",
        "log_loss_score",
        "spearman_corr",
        "mae",
        "calibration_table",
        "as_of_ratings_split",
    ],
    "sportsdataverse.cfb.cfb_projection_constants": ["brier_score", "spearman_corr", "mae"],
    "sportsdataverse.mbb.mbb_prediction_constants": [
        "brier_score",
        "log_loss_score",
        "spearman_corr",
        "mae",
        "calibration_table",
        "as_of_ratings_split",
    ],
    "sportsdataverse.nba.nba_prediction_constants": [
        "brier_score",
        "log_loss_score",
        "spearman_corr",
        "mae",
        "calibration_table",
        "as_of_ratings_split",
    ],
    "sportsdataverse.nba.nba_draft_constants": ["spearman_corr", "mae", "calibration_table"],
    "sportsdataverse.nba.nba_playtype_constants": ["spearman_corr", "mae"],
    "sportsdataverse.nba.nba_shot_value_constants": ["mae"],
    "sportsdataverse.nfl.nfl_prediction_constants": [
        "brier_score",
        "log_loss_score",
        "spearman_corr",
        "mae",
        "calibration_table",
    ],
    "sportsdataverse.nfl.nfl_projection_constants": ["brier_score", "log_loss_score", "spearman_corr", "mae"],
    "sportsdataverse.nfl.nfl_scheme_constants": ["brier_score", "log_loss_score", "spearman_corr", "mae"],
    "sportsdataverse.nfl.nfl_ngs_constants": ["spearman_corr", "mae"],
    "sportsdataverse.nhl.nhl_prediction_constants": [
        "brier_score",
        "log_loss_score",
        "spearman_corr",
        "mae",
        "calibration_table",
    ],
    "sportsdataverse.nhl.nhl_player_impact_constants": ["spearman_corr", "calibration_table"],
    "sportsdataverse.mlb.mlb_game_state_constants": ["brier_score", "spearman_corr", "mae", "calibration_table"],
    "sportsdataverse.mlb.mlb_hitting_constants": ["brier_score", "spearman_corr", "mae", "calibration_table"],
    "sportsdataverse.mlb.mlb_pitching_constants": ["spearman_corr", "mae", "calibration_table"],
    "sportsdataverse.mlb.mlb_run_values": ["spearman_corr", "mae"],
}


@pytest.mark.parametrize("modname,names", list(MIGRATED.items()), ids=list(MIGRATED))
def test_reexport_is_shared_object(modname, names):
    mod = importlib.import_module(modname)
    for nm in names:
        assert getattr(mod, nm) is getattr(core, nm), f"{modname}.{nm} is not the shared _common.metrics object"


def test_distinct_impls_were_left_alone():
    # nhl_microstat spearman has an n<2 guard; nfl_projection/nfl_scheme calibration_table
    # casts n->Int64 -- these are mathematically/schema distinct and must NOT be re-exports.
    from sportsdataverse.nhl import nhl_microstat_constants as micro
    from sportsdataverse.nfl import nfl_projection_constants as proj
    from sportsdataverse.nfl import nfl_scheme_constants as scheme

    assert micro.spearman_corr is not core.spearman_corr
    assert proj.calibration_table is not core.calibration_table
    assert scheme.calibration_table is not core.calibration_table
