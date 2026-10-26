from __future__ import annotations

# Sub-league packages — imported so ``sportsdataverse.providers.<leaf>`` is reachable
# as an attribute on this container module (0.0.65+).
from sportsdataverse.providers import cbs, espn_content, fox, thesportsdb  # noqa: F401,E402
from sportsdataverse.providers import yahoo  # noqa: F401,E402
