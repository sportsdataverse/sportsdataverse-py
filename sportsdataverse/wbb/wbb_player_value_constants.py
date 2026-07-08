"""Women's college basketball player-value harness (constants + fitters).

Thin shim over :mod:`sportsdataverse.mbb.mbb_player_value_constants` -- the
metric functions, numpy fitters, feature builder, and artifact I/O are
league-agnostic (every league-dependent entry point takes ``league=``), so
this module re-exports them **by reference** and callers pass
``league="womens"`` (or use :func:`get_player_value_constants
("womens")`` for the women's constants bundle).
"""

from __future__ import annotations

from sportsdataverse.mbb.mbb_player_value_constants import (
    PLAYER_VALUE_CONSTANTS as PLAYER_VALUE_CONSTANTS,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    PlayerValueConstants as PlayerValueConstants,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    aggregate_player_seasons as aggregate_player_seasons,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    as_of_season_split as as_of_season_split,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    bootstrap_ari as bootstrap_ari,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    get_player_value_constants as get_player_value_constants,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    kmeans_fit as kmeans_fit,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    load_artifact as load_artifact,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    logistic_fit as logistic_fit,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    mae as mae,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    player_per100_features as player_per100_features,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    ridge_cv_lambda as ridge_cv_lambda,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    ridge_fit as ridge_fit,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    roc_auc as roc_auc,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    save_artifact as save_artifact,
)
from sportsdataverse.mbb.mbb_player_value_constants import (
    spearman_corr as spearman_corr,
)

__all__ = [
    "PLAYER_VALUE_CONSTANTS",
    "PlayerValueConstants",
    "aggregate_player_seasons",
    "as_of_season_split",
    "bootstrap_ari",
    "get_player_value_constants",
    "kmeans_fit",
    "load_artifact",
    "logistic_fit",
    "mae",
    "player_per100_features",
    "ridge_cv_lambda",
    "ridge_fit",
    "roc_auc",
    "save_artifact",
    "spearman_corr",
]
