"""Upload status values and the transitions between them.

Statuses were previously bare strings scattered across three modules,
with the transition rules living implicitly in each claim_upload call.
Defining them here means a typo fails immediately, and the state machine
is readable in one place.
"""

from enum import StrEnum


class UploadStatus(StrEnum):
    """
    Status of an upload in the manifest.

    StrEnum so members compare equal to their string value — the manifest
    column stays plain text and existing rows keep working.
    """

    # Registration
    RECEIVED = "RECEIVED"
    DUPLICATE = "DUPLICATE"          # same contents as an earlier upload

    # Validation
    VALIDATING = "VALIDATING"
    VALIDATED = "VALIDATED"
    REJECTED = "REJECTED"            # file is wrong — needs a new file
    FAILED = "FAILED"                # something broke — retryable

    # Bronze
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    BRONZE_REJECTED = "BRONZE_REJECTED"
    BRONZE_FAILED = "BRONZE_FAILED"


# Which statuses each stage may claim from.
# The stale-claim logic in claim_upload also allows reclaiming the
# working status itself, so those are not listed here.
CLAIMABLE_FOR_VALIDATION = [UploadStatus.RECEIVED, UploadStatus.FAILED]
CLAIMABLE_FOR_BRONZE = [UploadStatus.VALIDATED, UploadStatus.BRONZE_FAILED]

# Nothing further happens to these.
TERMINAL_STATUSES = frozenset({
    UploadStatus.DUPLICATE,
    UploadStatus.REJECTED,
    UploadStatus.BRONZE_REJECTED,
    UploadStatus.PROCESSED,
})

# A run that dies leaves the row in one of these. The reaper resets them.
WORKING_STATUSES = frozenset({
    UploadStatus.VALIDATING,
    UploadStatus.PROCESSING,
})


def is_terminal(status: str) -> bool:
    """True if no further stage will act on this upload."""
    return status in TERMINAL_STATUSES