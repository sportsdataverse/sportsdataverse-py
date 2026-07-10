"""sportsdataverse.pwhl -- PWHL data loaders."""

from __future__ import annotations

from sportsdataverse.pwhl.pwhl_api import *  # noqa: F401,F403
from sportsdataverse.pwhl.pwhl_analytics import *  # noqa: F401,F403
from sportsdataverse.pwhl.pwhl_loaders import *
from sportsdataverse.pwhl.pwhl_loaders_extra import *
from sportsdataverse.pwhl.pwhl_player_impact import *  # noqa: F401,F403
from sportsdataverse.pwhl.pwhl_prediction_constants import (
    LEAGUE_CONSTANTS,
    LeagueConstants,
    as_of_ratings_split,
    brier_score,
    calibration_table,
    log_loss_score,
    mae,
    spearman_corr,
)

# NOTE: pwhl_prediction_constants.get_constants() is deliberately NOT
# re-exported here (bare name). sportsdataverse/__init__.py does
# `from sportsdataverse.nhl import *` then `from sportsdataverse.pwhl import
# *`; nhl's get_constants(league) and this module's zero-arg,
# PWHL-pinned get_constants() have different signatures, so re-exporting the
# bare name here would silently shadow nhl's version at the top-level
# sportsdataverse namespace. Access it as
# sportsdataverse.pwhl.pwhl_prediction_constants.get_constants() instead.
from sportsdataverse.pwhl.pwhl_team_ratings import pwhl_team_ratings
from sportsdataverse.pwhl.pwhl_market import pwhl_in_game_win_prob, pwhl_predict_games
from sportsdataverse.pwhl.pwhl_player_props import pwhl_game_total, pwhl_player_props
