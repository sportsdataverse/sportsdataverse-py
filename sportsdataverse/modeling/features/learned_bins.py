"""Learned discretization — fitted, auditable situational bins.

The reference ``LearnedDiscretizationEngine`` recipe: instead of hand-coded
``CASE WHEN`` gamestate buckets, fit a shallow decision tree on the
situational features and use its LEAVES as the bins. Each leaf carries a
human-readable rule string and the empirical class distribution, so the
binning is exactly as auditable as a hand-written table — but fitted.
Reference hyperparameters from the reference embeddings jobs: ``max_depth=7``,
``max_leaf_nodes=100``, ``min_samples_leaf=50``.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package;
feature builders and sim key-gens import from here.
"""

from __future__ import annotations

import dataclasses
from typing import Any, Dict, List, Sequence

import numpy as np
import polars as pl
from sklearn.tree import DecisionTreeClassifier


def _leaf_rules(tree: DecisionTreeClassifier, features: Sequence[str]) -> Dict[int, str]:
    """Human-readable constraint string per leaf node id."""
    t = tree.tree_
    rules: Dict[int, str] = {}

    def walk(node: int, constraints: List[str]) -> None:
        if t.children_left[node] == -1:  # leaf
            rules[node] = " and ".join(constraints) if constraints else "(root)"
            return
        name = features[t.feature[node]]
        threshold = float(t.threshold[node])
        walk(t.children_left[node], [*constraints, f"{name} <= {threshold:.4g}"])
        walk(t.children_right[node], [*constraints, f"{name} > {threshold:.4g}"])

    walk(0, [])
    return rules


@dataclasses.dataclass
class LearnedBins:
    """A fitted leaf-bin discretizer.

    Attributes:
        features: Situational feature columns the tree splits on.
        target: The categorical outcome column the bins were fit against.
        classes: Outcome class labels, column order of ``leaf_table`` probs.
        leaf_table: One row per leaf: ``leaf_id``, ``n``, ``rule``, and one
            ``p_{class}`` column per outcome class — the auditable artifact.
    """

    features: List[str]
    target: str
    classes: List[str]
    leaf_table: pl.DataFrame
    _tree: DecisionTreeClassifier = dataclasses.field(repr=False)

    def assign(self, df: pl.DataFrame) -> pl.Series:
        """Assign each row to its learned bin (leaf id).

        Args:
            df: Frame carrying the fitted feature columns.

        Returns:
            An Int64 series of leaf ids (usable as a shelf-key component).
        """
        matrix = df.select(self.features).drop_nulls().to_numpy().astype(float)
        if matrix.shape[0] != df.height:
            raise ValueError("assign() requires non-null feature values")
        return pl.Series("leaf_id", self._tree.apply(matrix).astype(np.int64))

    def decision_table(self) -> pl.DataFrame:
        """The fitted tree's split structure as a flat, walkable frame.

        One row per tree node: internal nodes carry ``feature`` /
        ``threshold`` / ``left`` / ``right`` (sklearn semantics —
        ``value <= threshold`` goes left), leaves carry nulls and
        ``is_leaf=True``. This is the serializable form a runtime keyer
        walks without holding the sklearn estimator.

        Returns:
            Frame with ``node_id``, ``feature``, ``threshold``, ``left``,
            ``right``, ``is_leaf`` sorted by ``node_id``.

        Example:
            Quick start::

                table = bins.decision_table()
                table.filter(pl.col("is_leaf") == False).head()
        """
        t = self._tree.tree_
        rows: List[Dict[str, Any]] = []
        for node in range(t.node_count):
            leaf = int(t.children_left[node]) == -1
            rows.append(
                {
                    "node_id": node,
                    "feature": None if leaf else self.features[int(t.feature[node])],
                    "threshold": None if leaf else float(t.threshold[node]),
                    "left": None if leaf else int(t.children_left[node]),
                    "right": None if leaf else int(t.children_right[node]),
                    "is_leaf": leaf,
                }
            )
        schema = {
            "node_id": pl.Int64,
            "feature": pl.Utf8,
            "threshold": pl.Float64,
            "left": pl.Int64,
            "right": pl.Int64,
            "is_leaf": pl.Boolean,
        }
        return pl.DataFrame(rows, schema=schema)


def fit_learned_bins(
    df: pl.DataFrame,
    *,
    features: Sequence[str],
    target: str,
    max_depth: int = 7,
    max_leaf_nodes: int = 100,
    min_samples_leaf: int = 50,
    random_state: int = 0,
) -> LearnedBins:
    """Fit the leaf-bin discretizer (reference hyperparameters).

    Args:
        df: Observations (rows with nulls in the used columns are dropped).
        features: Situational feature columns (numeric).
        target: Categorical outcome column.
        max_depth: Tree depth cap.
        max_leaf_nodes: Bin-count cap.
        min_samples_leaf: Minimum observations per bin — the coverage floor
            that keeps every learned bin estimable.
        random_state: Deterministic tie-breaking.

    Returns:
        The fitted :class:`LearnedBins`.

    Raises:
        ValueError: When no usable rows remain.

    Example:
        Fit gamestate bins from real possession events::

            from sportsdataverse.modeling.features.learned_bins import fit_learned_bins
            bins = fit_learned_bins(
                events, features=["score_diff", "period", "clock_seconds"],
                target="outcome", min_samples_leaf=25,
            )
            print(bins.leaf_table.sort("n", descending=True).head())
            keys = bins.assign(events)
    """
    clean = df.select([*features, target]).drop_nulls()
    if clean.height == 0:
        raise ValueError("no usable rows after dropping nulls")
    X = clean.select(features).to_numpy().astype(float)
    y = clean.get_column(target).cast(pl.Utf8).to_numpy()

    tree = DecisionTreeClassifier(
        max_depth=max_depth,
        max_leaf_nodes=max_leaf_nodes,
        min_samples_leaf=min_samples_leaf,
        random_state=random_state,
    )
    tree.fit(X, y)

    classes = [str(c) for c in tree.classes_]
    leaf_ids = tree.apply(X)
    probs = tree.predict_proba(X)
    rules = _leaf_rules(tree, list(features))

    rows: List[Dict[str, Any]] = []
    for leaf in sorted(set(int(v) for v in leaf_ids)):
        mask = leaf_ids == leaf
        row: Dict[str, Any] = {
            "leaf_id": leaf,
            "n": int(mask.sum()),
            "rule": rules.get(leaf, "?"),
        }
        leaf_probs = probs[mask][0]  # identical within a leaf
        for cls, p in zip(classes, leaf_probs):
            row[f"p_{cls}"] = float(p)
        rows.append(row)

    return LearnedBins(
        features=list(features),
        target=target,
        classes=classes,
        leaf_table=pl.DataFrame(rows),
        _tree=tree,
    )
