from __future__ import annotations

# Sub-league packages — imported so ``sportsdataverse.hockey.<leaf>`` is reachable
# as an attribute on this container module (0.0.65+).
from sportsdataverse.hockey import ahl, mch, ohl, qmjhl  # noqa: F401,E402
from sportsdataverse.hockey import wch, whl  # noqa: F401,E402

# HockeyTech leagues promoted from the recon registry to the callable surface
# (2026-07-12) — all live-verified, one shared parser. See hockeytech/_leagues.py.
from sportsdataverse.hockey import echl, sphl, chl  # noqa: F401,E402
from sportsdataverse.hockey import ushl, bchl, ajhl, sjhl, ojhl  # noqa: F401,E402
from sportsdataverse.hockey import cchl, gojhl, mhl, nojhl  # noqa: F401,E402
from sportsdataverse.hockey import vijhl, kijhl, mjhl  # noqa: F401,E402
