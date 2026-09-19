"""Data-integrity checks that ship in the wheel.

``tools/validation`` is the offline harness and does not ship, so the rule table
and the finding types every consumer needs live here:

* :mod:`sportsdataverse.validation.pbp_invariants` -- processor invariants over
  one game's plays (``evaluate``), shared with the offline sweep.
* :mod:`sportsdataverse.validation.findings` -- ``Finding`` / ``Severity`` /
  ``CheckContext``, the records the offline checks report against.
"""

from __future__ import annotations
