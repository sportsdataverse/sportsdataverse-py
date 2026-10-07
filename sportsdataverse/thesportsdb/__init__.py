from __future__ import annotations

# ``thesportsdb`` is a non-league home (like ``odds``). The generated flat module and its parser
# are re-exported by hand -- generate.py renders thesportsdb.py but never edits this file
# (_ensure_init_import only covers ESPN league extension modules). Those two import lines are
# added below now that thesportsdb.py has been rendered; this package is star-imported by
# sportsdataverse/__init__.py, so these lines may only exist once the module does.
from sportsdataverse.thesportsdb.thesportsdb import *  # noqa: F401,F403
from sportsdataverse.thesportsdb.thesportsdb_parsers import *  # noqa: F401,F403
