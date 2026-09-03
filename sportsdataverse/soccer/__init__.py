from __future__ import annotations

from sportsdataverse.soccer.soccer_espn_ext import *  # noqa: F401,F403

# Flat-API families homed directly at ``sportsdataverse.soccer``.
from sportsdataverse.soccer.asa import *  # noqa: F401,F403
from sportsdataverse.soccer.asa_parsers import *  # noqa: F401,F403

# Sub-league packages — imported so ``sportsdataverse.soccer.<leaf>`` is reachable
# as an attribute on this container module (0.0.65+).
from sportsdataverse.soccer import bundesliga, epl, laliga, ligamx  # noqa: F401,E402
from sportsdataverse.soccer import ligue1, mls, nwsl, seriea  # noqa: F401,E402
from sportsdataverse.soccer import ucl, uel, wc, wwc  # noqa: F401,E402
