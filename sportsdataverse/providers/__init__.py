from __future__ import annotations

# Sub-league packages — imported so ``sportsdataverse.providers.<leaf>`` is reachable
# as an attribute on this container module (0.0.65+).
from sportsdataverse.providers import cbs, espn_content, espn_fantasy, fox  # noqa: F401,E402
from sportsdataverse.providers import thesportsdb, yahoo  # noqa: F401,E402
