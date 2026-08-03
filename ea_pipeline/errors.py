"""Exception types that map to manifest statuses."""


class ContractViolation(Exception):
    """
    The file itself is wrong: unreadable, empty, bad delimiter, or it
    breaks the column contract.

    Retrying the same file will not help — a new file is needed.
    Maps to status REJECTED.
    """


class BronzeIngestionError(Exception):
    """
    The file cannot be loaded to bronze as it stands.

    Maps to status BRONZE_REJECTED.
    """