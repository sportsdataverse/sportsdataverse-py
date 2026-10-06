from __future__ import annotations

# ``espn_content`` is a non-league home (like ``odds``). The generated flat module and its parser
# are re-exported by hand -- generate.py renders espn_content.py but never edits this file
# (_ensure_init_import only covers ESPN league extension modules). Those two import lines are
# added once the flat module has been rendered, because this package is star-imported by
# sportsdataverse/__init__.py and importing a not-yet-generated module would break every import.
