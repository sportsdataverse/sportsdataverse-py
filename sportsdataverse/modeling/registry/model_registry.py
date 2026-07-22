"""Lightweight model registry — lineage + feature-drift detection.

The adapted role of the model-training-service's model-initialization config,
kept deliberately thin: a :class:`ModelCard` records a model's identity, its
input features, the training script, the trained seasons, and its metrics —
plus a **feature fingerprint** (the stable per-column SHA256s from
:func:`~sportsdataverse.modeling.integrity.publish_audit.fingerprint_frame`).

Its point is :func:`feature_drift`: recompute the fingerprint against the
current feature frame and a model's inputs that silently changed upstream —
the floating-``@main`` dependency incident, where a published column's engine
flipped with no commit — surface as drifted columns at train time instead of
as mystery output drift downstream. The manifest is plain JSON so it lives
naturally next to the artifacts in the release store, not in runtime code.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Dict, Iterable, List, Union

import polars as pl

from sportsdataverse.modeling.integrity.publish_audit import fingerprint_frame


@dataclasses.dataclass(frozen=True)
class ModelCard:
    """One model's registry entry.

    Attributes:
        name: Registry key (unique per manifest).
        source_features: Input feature columns the model was trained on.
        feature_fingerprint: ``{column: sha256}`` of the training features
            (stable per-column content hashes; no timestamps).
        training_script: Path/identifier of the script that trained it.
        trained_seasons: Seasons the training data spanned.
        metrics: Reported evaluation metrics.
    """

    name: str
    source_features: List[str]
    feature_fingerprint: Dict[str, str]
    training_script: str
    trained_seasons: List[int]
    metrics: Dict[str, float]


def feature_fingerprint(frame: pl.DataFrame, features: Iterable[str]) -> Dict[str, str]:
    """Stable per-column SHA256s for a frame's feature columns.

    Args:
        frame: The feature frame.
        features: Columns to fingerprint.

    Returns:
        ``{column: sha256}`` (the deterministic hashes only — the
        timestamp/git fields of the full fingerprint are dropped).

    Raises:
        ValueError: When a feature column is absent.

    Example:
        Quick start::

            from sportsdataverse.modeling.registry import feature_fingerprint
            feature_fingerprint(features, ["fga", "fg3a"])
    """
    features = list(features)
    missing = [c for c in features if c not in frame.columns]
    if missing:
        raise ValueError(f"feature columns absent from frame: {missing}")
    fp = fingerprint_frame(frame.select(features))
    return {c: str(fp["columns"][c]["sha256"]) for c in features}


def make_card(
    name: str,
    frame: pl.DataFrame,
    *,
    features: Iterable[str],
    training_script: str,
    trained_seasons: Iterable[int],
    metrics: Dict[str, float],
) -> ModelCard:
    """Build a :class:`ModelCard`, fingerprinting the training features.

    Args:
        name: Registry key.
        frame: The training feature frame.
        features: Input feature columns.
        training_script: Script path/identifier.
        trained_seasons: Seasons the training data spanned.
        metrics: Reported evaluation metrics.

    Returns:
        The card, with ``feature_fingerprint`` computed from ``frame``.

    Example:
        Quick start::

            from sportsdataverse.modeling.registry import make_card
            card = make_card("nba_scorer", features_df, features=["fga", "fg3a"],
                             training_script="train_nba.py", trained_seasons=[2023],
                             metrics={"brier": 0.19})
    """
    features = list(features)
    return ModelCard(
        name=name,
        source_features=features,
        feature_fingerprint=feature_fingerprint(frame, features),
        training_script=training_script,
        trained_seasons=[int(s) for s in trained_seasons],
        metrics={str(k): float(v) for k, v in metrics.items()},
    )


def feature_drift(card: ModelCard, frame: pl.DataFrame) -> List[str]:
    """Columns whose current content differs from the card's fingerprint.

    Args:
        card: A registered :class:`ModelCard`.
        frame: The current feature frame.

    Returns:
        The drifted ``source_features`` — a column is drifted when it is
        missing from ``frame`` or its content hash differs from the stored
        fingerprint. Empty means the model's inputs are unchanged.

    Example:
        Guard a re-train / re-publish::

            drift = feature_drift(card, current_features)
            if drift:
                raise RuntimeError(f"upstream feature drift: {drift}")
    """
    present = [c for c in card.source_features if c in frame.columns]
    current = feature_fingerprint(frame, present) if present else {}
    drifted: List[str] = []
    for col in card.source_features:
        if col not in frame.columns or current.get(col) != card.feature_fingerprint.get(col):
            drifted.append(col)
    return drifted


def write_manifest(path: Union[str, Path], cards: Iterable[ModelCard]) -> None:
    """Write model cards to a JSON manifest keyed by name.

    Args:
        path: Destination ``.json`` path.
        cards: The cards to persist.

    Raises:
        ValueError: On a duplicate card name.

    Example:
        Quick start::

            write_manifest("models_manifest.json", [card_a, card_b])
    """
    manifest: Dict[str, Dict] = {}
    for card in cards:
        if card.name in manifest:
            raise ValueError(f"duplicate model name in manifest: {card.name!r}")
        manifest[card.name] = {k: v for k, v in dataclasses.asdict(card).items() if k != "name"}
    Path(path).write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n", encoding="utf-8")


def read_manifest(path: Union[str, Path]) -> Dict[str, ModelCard]:
    """Read a JSON manifest back into ``{name: ModelCard}``.

    Args:
        path: The manifest path.

    Returns:
        The cards keyed by name.

    Example:
        Quick start::

            cards = read_manifest("models_manifest.json")
            cards["nba_scorer"].trained_seasons
    """
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    out: Dict[str, ModelCard] = {}
    for name, entry in raw.items():
        out[name] = ModelCard(
            name=name,
            source_features=list(entry["source_features"]),
            feature_fingerprint=dict(entry["feature_fingerprint"]),
            training_script=str(entry["training_script"]),
            trained_seasons=[int(s) for s in entry["trained_seasons"]],
            metrics={str(k): float(v) for k, v in entry["metrics"].items()},
        )
    return out
