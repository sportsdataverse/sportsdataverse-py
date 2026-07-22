"""Back-compat shim: moved to :mod:`sportsdataverse.modeling.features.mixed_effects`.

Importing from ``sportsdataverse._common.mixed_effects`` still works (these are the
same objects, re-exported) but the canonical home is now the ``modeling``
subpackage. New code should import from ``sportsdataverse.modeling.features``.
"""

from sportsdataverse.modeling.features.mixed_effects import (
    RandomInterceptFit as RandomInterceptFit,
    fit_random_intercepts as fit_random_intercepts,
    shrunk_group_means as shrunk_group_means,
)
