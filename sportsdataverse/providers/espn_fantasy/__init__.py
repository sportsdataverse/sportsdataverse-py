from __future__ import annotations

# ``espn_fantasy`` is a cross-sport provider home (like ``espn_content``). generate.py renders
# espn_fantasy.py but never edits this file, so the generated module and its parsers are
# re-exported here by hand; ``sportsdataverse.providers`` imports this package.
from sportsdataverse.providers.espn_fantasy.espn_fantasy import *  # noqa: F401,F403
from sportsdataverse.providers.espn_fantasy.espn_fantasy_parsers import *  # noqa: F401,F403
