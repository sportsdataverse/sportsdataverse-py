"""Data-integrity gates that ship in the wheel.

``tools/validation`` is the offline harness; it does not ship. Anything a
release build stage, the Data API or Game on Paper has to call lives here:

* :func:`validate_game` -- the per-game gate, returning a :class:`GameReport`.
* :mod:`sportsdataverse.validation.pbp_invariants` -- the invariant rule table
  (``evaluate``), shared with the offline sweep so there is one rule set.
* :mod:`sportsdataverse.validation.findings` -- the harness ``Finding`` /
  ``Severity`` / ``CheckContext`` types the offline checks record against.
"""

from __future__ import annotations

from sportsdataverse.validation.report import (
    RULE_SCOPE,
    Finding,
    GameReport,
    Rule,
    validate_game,
)

__all__ = ["RULE_SCOPE", "Finding", "GameReport", "Rule", "validate_game"]
