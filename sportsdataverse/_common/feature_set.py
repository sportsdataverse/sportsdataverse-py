"""Back-compat shim: moved to :mod:`sportsdataverse.modeling.features.feature_set`.

Importing from ``sportsdataverse._common.feature_set`` still works (these are the
same objects, re-exported) but the canonical home is now the ``modeling``
subpackage. New code should import from ``sportsdataverse.modeling.features``.
"""

from sportsdataverse.modeling.features.feature_set import (
    AsOf as AsOf,
    FeatureSetSpec as FeatureSetSpec,
    feature_column_names as feature_column_names,
    splits_grid as splits_grid,
    rolling_features as rolling_features,
    as_of_features as as_of_features,
    fit_span_blend as fit_span_blend,
)
