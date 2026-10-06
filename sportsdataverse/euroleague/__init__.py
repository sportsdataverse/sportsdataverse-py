"""EuroLeague / EuroCup wrappers (``euroleague_*``) over the three keyless EuroLeague APIs.

Unofficial, keyless API; not supported by Euroleague Basketball. Wrap-only: payloads are
not redistributed as release assets.
"""

from __future__ import annotations

# ``euroleague`` is a non-league home (like ``odds``): the generated flat module and its
# parser are re-exported by hand here; generate.py renders euroleague.py but does not edit this file.
from sportsdataverse.euroleague.euroleague import *  # noqa: F401,F403
from sportsdataverse.euroleague.euroleague_parsers import *  # noqa: F401,F403
