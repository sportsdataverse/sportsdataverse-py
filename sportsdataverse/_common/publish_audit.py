"""Back-compat shim: moved to :mod:`sportsdataverse.modeling.integrity.publish_audit`.

Importing from ``sportsdataverse._common.publish_audit`` still works (these are the
same objects, re-exported) but the canonical home is now the ``modeling``
subpackage. New code should import from ``sportsdataverse.modeling.integrity``.
"""

from sportsdataverse.modeling.integrity.publish_audit import (
    FINGERPRINT_VERSION as FINGERPRINT_VERSION,
    FINGERPRINT_SUFFIX as FINGERPRINT_SUFFIX,
    DEFAULT_MEAN_SHIFT_SIGMA as DEFAULT_MEAN_SHIFT_SIGMA,
    DEFAULT_NULL_RATE_DELTA as DEFAULT_NULL_RATE_DELTA,
    DEFAULT_SHRINK_TOLERANCE as DEFAULT_SHRINK_TOLERANCE,
    PublishAudit as PublishAudit,
    fingerprint_frame as fingerprint_frame,
    fingerprint_parquet as fingerprint_parquet,
    write_fingerprint as write_fingerprint,
    read_fingerprint as read_fingerprint,
    drift_report as drift_report,
    completeness_report as completeness_report,
    audit_asset as audit_asset,
    append_manifest as append_manifest,
)
