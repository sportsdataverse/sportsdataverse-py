"""Publish-integrity + source-agreement — fingerprints, drift, completeness,
cross-provider reconciliation."""

from __future__ import annotations

from sportsdataverse.modeling.integrity.completeness import (
    ScheduleCompleteness,
    schedule_completeness,
    schedule_frame_from_espn_scoreboard,
)
from sportsdataverse.modeling.integrity.contracts import (
    ColumnContract,
    ContractReport,
    ContractViolation,
    DataContract,
    derive_contract,
    read_contract,
    validate_frame,
    write_contract,
)
from sportsdataverse.modeling.integrity.publish_audit import (
    DEFAULT_MEAN_SHIFT_SIGMA,
    DEFAULT_NULL_RATE_DELTA,
    DEFAULT_SHRINK_TOLERANCE,
    FINGERPRINT_SUFFIX,
    FINGERPRINT_VERSION,
    PublishAudit,
    append_manifest,
    audit_asset,
    completeness_report,
    drift_report,
    fingerprint_frame,
    fingerprint_parquet,
    read_fingerprint,
    write_fingerprint,
)
from sportsdataverse.modeling.integrity.source_agreement import (
    agreement_summary,
    key_coverage,
    reconcile,
)

__all__ = [
    "ScheduleCompleteness",
    "schedule_completeness",
    "schedule_frame_from_espn_scoreboard",
    "ColumnContract",
    "ContractReport",
    "ContractViolation",
    "DataContract",
    "derive_contract",
    "read_contract",
    "validate_frame",
    "write_contract",
    "DEFAULT_MEAN_SHIFT_SIGMA",
    "DEFAULT_NULL_RATE_DELTA",
    "DEFAULT_SHRINK_TOLERANCE",
    "FINGERPRINT_SUFFIX",
    "FINGERPRINT_VERSION",
    "PublishAudit",
    "agreement_summary",
    "append_manifest",
    "audit_asset",
    "completeness_report",
    "drift_report",
    "fingerprint_frame",
    "fingerprint_parquet",
    "key_coverage",
    "read_fingerprint",
    "reconcile",
    "write_fingerprint",
]
