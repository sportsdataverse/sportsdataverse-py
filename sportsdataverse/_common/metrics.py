"""Back-compat shim: moved to :mod:`sportsdataverse.modeling.eval.metrics`.

Importing from ``sportsdataverse._common.metrics`` still works (these are the
same objects, re-exported) but the canonical home is now the ``modeling``
subpackage. New code should import from ``sportsdataverse.modeling.eval``.
"""

from sportsdataverse.modeling.eval.metrics import (
    brier_score as brier_score,
    log_loss_score as log_loss_score,
    spearman_corr as spearman_corr,
    mae as mae,
    calibration_table as calibration_table,
    as_of_ratings_split as as_of_ratings_split,
    BaselineResult as BaselineResult,
    baseline_test as baseline_test,
    group_error_metrics as group_error_metrics,
)
