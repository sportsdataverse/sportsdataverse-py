"""Back-compat shim: moved to :mod:`sportsdataverse.modeling.features.train_guards`.

Importing from ``sportsdataverse._common.train_guards`` still works (these are the
same objects, re-exported) but the canonical home is now the ``modeling``
subpackage. New code should import from ``sportsdataverse.modeling.features``.
"""

from sportsdataverse.modeling.features.train_guards import (
    audit_training_frame as audit_training_frame,
    drop_constant_columns as drop_constant_columns,
    correlation_prune as correlation_prune,
)
