"""Back-compat shim: moved to :mod:`sportsdataverse.modeling.features.learned_bins`.

Importing from ``sportsdataverse._common.learned_bins`` still works (these are the
same objects, re-exported) but the canonical home is now the ``modeling``
subpackage. New code should import from ``sportsdataverse.modeling.features``.
"""

from sportsdataverse.modeling.features.learned_bins import (
    LearnedBins as LearnedBins,
    fit_learned_bins as fit_learned_bins,
)
