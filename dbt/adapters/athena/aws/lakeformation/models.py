from typing import Dict, List, Optional

from mypy_boto3_lakeformation.type_defs import DataCellsFilterTypeDef
from pydantic import BaseModel


class FilterConfig(BaseModel):
    row_filter: str
    column_names: List[str] = []
    principals: List[str] = []

    def to_api_repr(self, catalog_id: str, database: str, table: str, name: str) -> DataCellsFilterTypeDef:
        return {
            "TableCatalogId": catalog_id,
            "DatabaseName": database,
            "TableName": table,
            "Name": name,
            "RowFilter": {"FilterExpression": self.row_filter},
            "ColumnNames": self.column_names,
            "ColumnWildcard": {"ExcludedColumnNames": []},
        }

    def to_update(self, existing: DataCellsFilterTypeDef) -> bool:
        return self.row_filter != existing["RowFilter"]["FilterExpression"] or set(self.column_names) != set(
            existing["ColumnNames"]
        )


class DataCellFiltersConfig(BaseModel):
    enabled: bool = False
    filters: Dict[str, FilterConfig]


class LfTagsConfig(BaseModel):
    enabled: bool = False
    tags: Optional[Dict[str, str]] = None
    tags_columns: Optional[Dict[str, Dict[str, List[str]]]] = None


class LfGrantsConfig(BaseModel):
    data_cell_filters: DataCellFiltersConfig
