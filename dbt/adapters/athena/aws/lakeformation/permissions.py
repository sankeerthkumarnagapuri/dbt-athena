from typing import Dict

from mypy_boto3_lakeformation import LakeFormationClient
from mypy_boto3_lakeformation.type_defs import (
    BatchPermissionsRequestEntryTypeDef,
    DataCellsFilterTypeDef,
)

from dbt.adapters.athena.aws.lakeformation.models import LfGrantsConfig
from dbt.adapters.athena.constants import LOGGER
from dbt.adapters.athena.dbt.relation import AthenaRelation


class LfPermissions:
    def __init__(self, catalog_id: str, relation: AthenaRelation, lf_client: LakeFormationClient) -> None:
        self.catalog_id = catalog_id
        self.relation = relation
        self.database: str = relation.schema
        self.table: str = relation.identifier
        self.lf_client = lf_client

    def get_filters(self) -> Dict[str, DataCellsFilterTypeDef]:
        table_resource = {"CatalogId": self.catalog_id, "DatabaseName": self.database, "Name": self.table}
        return {f["Name"]: f for f in self.lf_client.list_data_cells_filter(Table=table_resource)["DataCellsFilters"]}

    def process_filters(self, config: LfGrantsConfig) -> None:
        current_filters = self.get_filters()
        LOGGER.debug(f"CURRENT FILTERS: {current_filters}")

        to_drop = [f for name, f in current_filters.items() if name not in config.data_cell_filters.filters]
        LOGGER.debug(f"FILTERS TO DROP: {to_drop}")
        for f in to_drop:
            self.lf_client.delete_data_cells_filter(
                TableCatalogId=f["TableCatalogId"],
                DatabaseName=f["DatabaseName"],
                TableName=f["TableName"],
                Name=f["Name"],
            )

        to_add = [
            f.to_api_repr(self.catalog_id, self.database, self.table, name)
            for name, f in config.data_cell_filters.filters.items()
            if name not in current_filters
        ]
        LOGGER.debug(f"FILTERS TO ADD: {to_add}")
        for f in to_add:
            self.lf_client.create_data_cells_filter(TableData=f)

        to_update = [
            f.to_api_repr(self.catalog_id, self.database, self.table, name)
            for name, f in config.data_cell_filters.filters.items()
            if name in current_filters and f.to_update(current_filters[name])
        ]
        LOGGER.debug(f"FILTERS TO UPDATE: {to_update}")
        for f in to_update:
            self.lf_client.update_data_cells_filter(TableData=f)

    def process_permissions(self, config: LfGrantsConfig) -> None:
        for name, f in config.data_cell_filters.filters.items():
            LOGGER.debug(f"Start processing permissions for filter: {name}")
            current_permissions = self.lf_client.list_permissions(
                Resource={
                    "DataCellsFilter": {
                        "TableCatalogId": self.catalog_id,
                        "DatabaseName": self.database,
                        "TableName": self.table,
                        "Name": name,
                    }
                }
            )["PrincipalResourcePermissions"]

            current_principals = {p["Principal"]["DataLakePrincipalIdentifier"] for p in current_permissions}

            to_revoke = {p for p in current_principals if p not in f.principals}
            if to_revoke:
                self.lf_client.batch_revoke_permissions(
                    CatalogId=self.catalog_id,
                    Entries=[self._permission_entry(name, principal, idx) for idx, principal in enumerate(to_revoke)],
                )
                revoke_principals_msg = "\n".join(to_revoke)
                LOGGER.debug(f"Revoked permissions for filter {name} from principals:\n{revoke_principals_msg}")
            else:
                LOGGER.debug(f"No redundant permissions found for filter: {name}")

            to_add = {p for p in f.principals if p not in current_principals}
            if to_add:
                self.lf_client.batch_grant_permissions(
                    CatalogId=self.catalog_id,
                    Entries=[self._permission_entry(name, principal, idx) for idx, principal in enumerate(to_add)],
                )
                add_principals_msg = "\n".join(to_add)
                LOGGER.debug(f"Granted permissions for filter {name} to principals:\n{add_principals_msg}")
            else:
                LOGGER.debug(f"No new permissions added for filter {name}")

            LOGGER.debug(f"Permissions are set to be consistent with config for filter: {name}")

    def _permission_entry(self, filter_name: str, principal: str, idx: int) -> BatchPermissionsRequestEntryTypeDef:
        return {
            "Id": str(idx),
            "Principal": {"DataLakePrincipalIdentifier": principal},
            "Resource": {
                "DataCellsFilter": {
                    "TableCatalogId": self.catalog_id,
                    "DatabaseName": self.database,
                    "TableName": self.table,
                    "Name": filter_name,
                }
            },
            "Permissions": ["SELECT"],
            "PermissionsWithGrantOption": [],
        }
