"""Cross-sport modeling toolkit — the SDV MLOps library.

Homes the model-lifecycle capabilities adapted from the reference stack into one
discoverable subpackage, grouped by concern:

* :mod:`~sportsdataverse.modeling.integrity` — publish fingerprints, drift,
  export-completeness, and cross-provider source-agreement;
* :mod:`~sportsdataverse.modeling.eval` — metrics, the beat-the-baseline gate,
  the experiment ledger, and season backtests;
* :mod:`~sportsdataverse.modeling.features` — the windowed aggregation engine,
  learned discretization, training guards, and mixed-effects pooling;
* :mod:`~sportsdataverse.modeling.registry` — model lineage cards and
  feature-drift detection.

The pre-move ``sportsdataverse._common.<module>`` import paths still work via
thin re-export shims; ``modeling`` is the canonical home.
"""

from __future__ import annotations

from sportsdataverse.modeling import eval, features, integrity, registry

__all__ = ["integrity", "eval", "features", "registry"]
