"""Load the schema from JSON and derive everything physical from it.

The JSON is the single source of truth. Dataset names, landing directories,
table names, column types, and validation contracts are all derived here, so
changing the model means editing one file rather than four.

Entities and relationships are treated identically: a relationship is just a
dataset whose rows happen to reference two other datasets.
"""

import json
from dataclasses import dataclass
from pathlib import Path

# The schema sits at the repo root, one level above the package.
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema" / "schema.json"


def _load_schema() -> dict:
    with open(SCHEMA_PATH) as f:
        return json.load(f)


SCHEMA = _load_schema()
PHYSICAL = SCHEMA["physical"]


# --- physical configuration, all derived from the JSON -------------------

CATALOG = PHYSICAL["catalog"]
LANDING_VOLUME = PHYSICAL["landing_volume"]
BRONZE_SCHEMA = PHYSICAL["bronze_schema"]
SILVER_SCHEMA = PHYSICAL["silver_schema"]
GOLD_SCHEMA = PHYSICAL["gold_schema"]
OPERATIONS_SCHEMA = PHYSICAL["operations_schema"]
MANIFEST_TABLE = PHYSICAL["manifest_table"]
LINK_FINDINGS_TABLE = PHYSICAL["link_findings_table"]

DATE_FORMAT = PHYSICAL["date_format"]
DATETIME_FORMAT = PHYSICAL["datetime_format"]

TYPE_MAPPING = PHYSICAL["type_mapping"]
SYSTEM_MANAGED_COLUMNS = set(PHYSICAL["system_managed_columns"])
TABLE_PROPERTIES = PHYSICAL["table_properties"]


@dataclass(frozen=True)
class ColumnSpec:
    """How one column moves from bronze to silver."""

    name: str
    data_type: str            # Spark type: string, date, decimal(18,2), ...
    required: bool = False
    description: str = ""
    enum_values: tuple[str, ...] = ()

    @property
    def is_typed(self) -> bool:
        """True when the column needs casting, and therefore _raw/_is_valid."""
        return self.data_type != "string"


@dataclass(frozen=True)
class DatasetContract:
    """Everything the pipeline needs to know about one dataset."""

    name: str
    display_name: str
    description: str
    table_type: str                    # "entity" or "relationship"
    key_columns: tuple[str, ...]
    columns: tuple[ColumnSpec, ...]
    reject_unexpected: bool = False

    # --- derived, so validation keeps working unchanged ---

    @property
    def required(self) -> frozenset[str]:
        return frozenset(c.name for c in self.columns if c.required)

    @property
    def optional(self) -> frozenset[str]:
        return frozenset(c.name for c in self.columns if not c.required)

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(c.name for c in self.columns)

    def __post_init__(self):
        names = [c.name for c in self.columns]

        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate column names in '{self.name}': {names}")

        if not self.key_columns:
            raise ValueError(f"Contract '{self.name}' has no key columns.")

        for key in self.key_columns:
            if key not in names:
                raise ValueError(
                    f"key column '{key}' is not a column of '{self.name}'"
                )


def _build_column(attribute: dict) -> ColumnSpec:
    """Turn one JSON attribute into a ColumnSpec."""
    json_type = attribute["data_type"]

    if json_type not in TYPE_MAPPING:
        raise ValueError(
            f"No type mapping for '{json_type}' on column "
            f"'{attribute['name']}'. Add it to physical.type_mapping."
        )

    name = attribute["name"]

    # System-managed columns are required in the target model but are not
    # something a person filling a spreadsheet will supply, so they are
    # optional on upload.
    required = bool(attribute.get("required")) and name not in SYSTEM_MANAGED_COLUMNS

    return ColumnSpec(
        name=name,
        data_type=TYPE_MAPPING[json_type],
        required=required,
        description=attribute.get("description", ""),
        enum_values=tuple(attribute.get("enum_values", ())),
    )


def _build_contract(definition: dict, table_type: str) -> DatasetContract:
    """Turn one JSON table or relationship into a DatasetContract."""
    if table_type == "entity":
        key_columns = tuple(definition["primary_key"])
    else:
        key_columns = tuple(definition["keys"]["primary_key"])

    return DatasetContract(
        name=definition["name"],
        display_name=definition.get("display_name", definition["name"]),
        description=definition.get("description", ""),
        table_type=table_type,
        key_columns=key_columns,
        columns=tuple(_build_column(a) for a in definition["attributes"]),
        reject_unexpected=PHYSICAL.get("reject_unexpected", False),
    )


def _build_all_contracts() -> dict[str, DatasetContract]:
    contracts = {}

    for table in SCHEMA["tables"]:
        contracts[table["name"]] = _build_contract(table, "entity")

    for relationship in SCHEMA["relationships"]:
        contracts[relationship["name"]] = _build_contract(relationship, "relationship")

    return contracts


DATASET_CONTRACTS: dict[str, DatasetContract] = _build_all_contracts()

DATASET_DIRECTORIES: dict[str, str] = {
    name: f"{LANDING_VOLUME}/{name}" for name in DATASET_CONTRACTS
}

BRONZE_TABLES: dict[str, str] = {
    name: f"{BRONZE_SCHEMA}.{name}" for name in DATASET_CONTRACTS
}

SILVER_TABLES: dict[str, str] = {
    name: f"{SILVER_SCHEMA}.{name}" for name in DATASET_CONTRACTS
}

