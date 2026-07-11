"""Internal cross-league infrastructure (T7.2).

Shared, league-agnostic rating-engine math that the per-sport prediction
spines delegate into: NFL's dense-design opponent-adjusted ridge, CFB's
dropped-level (``model.matrix``-style) opponent-adjusted ridge, and the
MBB/NBA/WBB/WNBA iterative (KenPom-style) fixed-point opponent adjustment.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package;
per-sport wrappers import from here and keep their own public signatures.
"""
