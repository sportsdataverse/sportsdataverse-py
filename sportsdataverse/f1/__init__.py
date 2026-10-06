from __future__ import annotations

# ``f1`` is a non-league home (like ``euroleague`` / ``odds``): the generated flat module
# (Jolpica F1, Ergast-compatible), its parser and the hand-written lap-time pager are
# re-exported by hand here; generate.py renders f1.py but does not edit this file.
#
# ``__all__`` is explicit: the top-level ``from sportsdataverse.f1 import *`` would
# otherwise copy the public name ``f1`` (the submodule ``sportsdataverse.f1.f1``) over
# the ``sportsdataverse.f1`` package attribute and leak the helper modules.
from sportsdataverse.f1 import f1, f1_extra, f1_parsers
from sportsdataverse.f1.f1 import *  # noqa: F401,F403
from sportsdataverse.f1.f1_extra import *  # noqa: F401,F403
from sportsdataverse.f1.f1_parsers import *  # noqa: F401,F403

__all__ = [*f1.__all__, *f1_extra.__all__, *f1_parsers.__all__]
