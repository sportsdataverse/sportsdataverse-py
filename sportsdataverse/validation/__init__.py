"""Data-integrity gates that ship in the wheel.

``tools/validation`` is the offline harness; it does not ship. Anything a
release build stage, the Data API or Game on Paper has to call lives here:

* :func:`validate_game` -- the per-game gate, returning a :class:`GameReport`.
* :mod:`sportsdataverse.validation.pbp_invariants` -- the invariant rule table
  (``evaluate``), shared with the offline sweep so there is one rule set.
* :mod:`sportsdataverse.validation.box_reconcile` -- the ``advBoxScore``
  section reconciliations (``box.*``), evaluated as part of the rule table.
* :mod:`sportsdataverse.validation.findings` -- the harness ``Finding`` /
  ``Severity`` / ``CheckContext`` types the offline checks record against.
* :mod:`sportsdataverse.validation.thresholds` -- the packaged tolerances
  (``DEFAULTS`` / ``for_league``), readable without PyYAML.
"""

from __future__ import annotations

from sportsdataverse.validation import thresholds
from sportsdataverse.validation.report import (
    NOT_APPLICABLE,
    RULE_SCOPE,
    SOURCE_COLUMNS,
    Finding,
    GameReport,
    Rule,
    validate_game,
)

__all__ = [
    "NOT_APPLICABLE",
    "RULE_SCOPE",
    "SOURCE_COLUMNS",
    "Finding",
    "GameReport",
    "Rule",
    "thresholds",
    "validate_game",
]
