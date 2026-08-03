"""File reading and hashing. Pure Python, no Spark."""

import csv
import hashlib
from dataclasses import dataclass

from ea_pipeline.errors import ContractViolation


@dataclass(frozen=True)
class CsvStructure:
    """What we learn from reading a CSV file's structure."""

    header: list[str]      # normalised column names
    row_count: int         # data rows, excluding the header


def calculate_file_hash(file_path: str, chunk_size: int = 1024 * 1024) -> str:
    """
    Calculate the SHA-256 hash of a file.

    Read in 1 MB chunks so large files never load entirely into memory.
    """
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            sha256.update(chunk)

    return sha256.hexdigest()


def normalise_column_name(column: str) -> str:
    """
    Make column names comparable, so 'Project ID' and 'project_id'
    are treated as the same column.
    """
    cleaned = column.strip().lower()

    for character in (" ", "-", ".", "/"):
        cleaned = cleaned.replace(character, "_")

    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")

    return cleaned.strip("_")


def read_csv_structure(file_path: str) -> CsvStructure:
    """
    Read a CSV file's header and count its data rows in one pass.

    Raises:
        ContractViolation: if the file cannot be read as valid CSV,
                           is empty, or has no data rows.
    """
    try:
        with open(
            file_path,
            mode="r",
            encoding="utf-8-sig",   # tolerates a UTF-8 byte-order mark
            newline="",
        ) as csv_file:
            reader = csv.reader(csv_file)

            try:
                raw_header = next(reader)
            except StopIteration:
                raise ContractViolation(
                    "The uploaded CSV file is completely empty."
                ) from None

            row_count = sum(1 for _ in reader)

    except UnicodeDecodeError:
        raise ContractViolation(
            "The file is not valid UTF-8. It was probably exported from an "
            "older version of Excel. Re-save it as 'CSV UTF-8'."
        ) from None

    if row_count == 0:
        raise ContractViolation(
            "The CSV file has a header row but no data rows."
        )

    header = [normalise_column_name(column) for column in raw_header]

    if not header:
        raise ContractViolation("The CSV file does not contain a header.")

    # A single column usually means the file is not comma-separated.
    if len(header) == 1 and any(sep in header[0] for sep in (";", "\t", "|")):
        raise ContractViolation(
            "The file does not appear to be comma-separated. Its whole header "
            "was read as a single column. Re-export it as a standard "
            "comma-delimited CSV."
        )

    if any(column == "" for column in header):
        raise ContractViolation("The CSV header contains a blank column name.")

    return CsvStructure(header=header, row_count=row_count)
