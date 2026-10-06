"""sdv-docs: an MCP server answering exact questions about the SportsDataverse surface.

This package must never import ``sportsdataverse``: its package ``__init__`` loads
every league (~4-12 s, ~320 MB), which an always-on agent server cannot afford.
"""
