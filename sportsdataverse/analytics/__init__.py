"""Cross-sport analytics helpers: rolling form windows, rate curves, defense vs position, and the Paper Index.

They moved here from the package top level; ``sportsdataverse.rolling_windows`` and the other old paths still
import, with a DeprecationWarning.
"""

from sportsdataverse.analytics import defense_vs_position, metric_curves, paper_index, rolling_windows  # noqa: F401
