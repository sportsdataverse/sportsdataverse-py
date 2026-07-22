"""Feature engineering — the windowed aggregation engine, learned
discretization, training guards, and mixed-effects partial pooling."""

from __future__ import annotations

from sportsdataverse.modeling.features.feature_set import (
    AsOf,
    FeatureSetSpec,
    feature_column_names,
    splits_grid,
    rolling_features,
    as_of_features,
    fit_span_blend,
)
from sportsdataverse.modeling.features.learned_bins import LearnedBins, fit_learned_bins
from sportsdataverse.modeling.features.mixed_effects import (
    PooledInterceptFit,
    RandomInterceptFit,
    fit_pooled_intercepts,
    fit_random_intercepts,
    shrunk_group_means,
)
from sportsdataverse.modeling.features.train_guards import (
    audit_training_frame,
    drop_constant_columns,
    correlation_prune,
)

__all__ = [
    "AsOf",
    "FeatureSetSpec",
    "LearnedBins",
    "PooledInterceptFit",
    "RandomInterceptFit",
    "as_of_features",
    "audit_training_frame",
    "correlation_prune",
    "drop_constant_columns",
    "feature_column_names",
    "fit_learned_bins",
    "fit_pooled_intercepts",
    "fit_random_intercepts",
    "fit_span_blend",
    "rolling_features",
    "shrunk_group_means",
    "splits_grid",
]
