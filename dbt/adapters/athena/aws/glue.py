from mypy_boto3_glue.type_defs import TableTypeDef

from dbt.adapters.athena.constants import LOGGER
from dbt.adapters.athena.dbt.relation import TableType

GLUE_TABLE_TYPE_TO_RELATION_MAPPING = {
    "EXTERNAL_TABLE": TableType.TABLE,
    "EXTERNAL": TableType.TABLE,  # type returned by federated query tables
    "MANAGED_TABLE": TableType.TABLE,
    "VIRTUAL_VIEW": TableType.VIEW,
    "table": TableType.TABLE,
    "view": TableType.VIEW,
    "cte": TableType.CTE,
    "materializedview": TableType.MATERIALIZED_VIEW,
}


def get_table_type(table: TableTypeDef) -> TableType:
    _type = GLUE_TABLE_TYPE_TO_RELATION_MAPPING.get(table.get("TableType"))
    _specific_type = table.get("Parameters", {}).get("table_type", "")

    if _specific_type.lower() == "iceberg":
        _type = TableType.ICEBERG

    if _type is None:
        raise ValueError("Table type cannot be None")

    LOGGER.debug(f"table_name : {table.get('Name')}")
    LOGGER.debug(f"table type : {_type}")

    return _type
