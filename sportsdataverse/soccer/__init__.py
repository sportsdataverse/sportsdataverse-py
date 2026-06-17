from __future__ import annotations

from sportsdataverse.soccer.soccer_espn_ext import *  # noqa: F401,F403

# Sub-league packages — imported so ``sportsdataverse.soccer.epl`` etc. are
# reachable as attributes on this module (0.0.65+).
from sportsdataverse.soccer import epl, laliga, bundesliga, seriea  # noqa: F401,E402
from sportsdataverse.soccer import ligue1, mls, ligamx, ucl, uel  # noqa: F401,E402
from sportsdataverse.soccer import nwsl, wwc, wc  # noqa: F401,E402
