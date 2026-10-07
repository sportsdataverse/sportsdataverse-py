from __future__ import annotations

# ``espn_content`` is a non-league home (like ``odds``). The generated flat module and its parser
# are re-exported by hand -- generate.py renders espn_content.py but never edits this file
# (_ensure_init_import only covers ESPN league extension modules). Those two import lines are
# added below now that espn_content.py has been rendered; this package is star-imported by
# sportsdataverse/__init__.py, so these lines may only exist once the module does.
from sportsdataverse.espn_content.espn_content import *  # noqa: F401,F403
from sportsdataverse.espn_content.espn_content_parsers import *  # noqa: F401,F403
