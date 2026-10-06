from __future__ import annotations

# ``f1`` is a non-league home (like ``euroleague`` / ``odds``): the generated flat module
# (Jolpica F1, Ergast-compatible), its parser and the hand-written lap-time pager are
# re-exported by hand here; generate.py renders f1.py but does not edit this file.
from sportsdataverse.f1.f1 import *  # noqa: F401,F403
from sportsdataverse.f1.f1_extra import *  # noqa: F401,F403
from sportsdataverse.f1.f1_parsers import *  # noqa: F401,F403
