from typing import Dict, List, Optional, Union

from mypy_boto3_lakeformation import LakeFormationClient
from mypy_boto3_lakeformation.type_defs import (
    AddLFTagsToResourceResponseTypeDef,
    GetResourceLFTagsResponseTypeDef,
    RemoveLFTagsFromResourceResponseTypeDef,
    ResourceTypeDef,
)
from pyathena.connection import Connection as AthenaConnection

from dbt.adapters.athena.aws.client_provider import AWSClientProvider
from dbt.adapters.athena.aws.lakeformation.models import LfTagsConfig
from dbt.adapters.athena.constants import LOGGER
from dbt.adapters.athena.dbt.relation import AthenaRelation
from dbt.exceptions import DbtRuntimeError


class LfTagsManager:
    def __init__(self):
        self._client_provider = AWSClientProvider[LakeFormationClient]("lakeformation")

    def _setup(self, connection: AthenaConnection, relation: AthenaRelation, lf_tags: Dict[str, str]):
        config = LfTagsConfig(enabled=True, tags=lf_tags)
        self._lf_client = self._client_provider.get_service_client(connection)
        self._database = relation.schema
        self._table = relation.identifier
        self._lf_tags = config.tags
        self._lf_tags_columns = config.tags_columns

    def process_lf_tags_database(
        self, connection: AthenaConnection, relation: AthenaRelation, lf_tags_database: Dict[str, str]
    ) -> None:
        self._setup(connection, relation, lf_tags_database)
        database_resource = {"Database": {"Name": relation.schema}}
        response = self._lf_client.add_lf_tags_to_resource(
            Resource=database_resource,
            LFTags=[{"TagKey": k, "TagValues": [v]} for k, v in self._lf_tags.items()],
        )
        self._parse_and_log_lf_response(response, None, self._lf_tags)

    def process_lf_tags(self, connection: AthenaConnection, relation: AthenaRelation, lf_tags: Dict[str, str]) -> None:
        self._setup(connection, relation, lf_tags)
        table_resource = {"Table": {"DatabaseName": self._database, "Name": self._table}}
        existing_lf_tags = self._lf_client.get_resource_lf_tags(Resource=table_resource)
        self._remove_lf_tags_columns(existing_lf_tags)
        self._apply_lf_tags_table(table_resource, existing_lf_tags)
        self._apply_lf_tags_columns()

    def _remove_lf_tags_columns(self, existing_lf_tags: GetResourceLFTagsResponseTypeDef) -> None:
        lf_tags_columns = existing_lf_tags.get("LFTagsOnColumns", [])
        LOGGER.debug(f"COLUMNS: {lf_tags_columns}")
        if lf_tags_columns:
            to_remove = {}
            for column in lf_tags_columns:
                for tag in column["LFTags"]:
                    tag_key = tag["TagKey"]
                    tag_value = tag["TagValues"][0]
                    if tag_key not in to_remove:
                        to_remove[tag_key] = {tag_value: [column["Name"]]}
                    elif tag_value not in to_remove[tag_key]:
                        to_remove[tag_key][tag_value] = [column["Name"]]
                    else:
                        to_remove[tag_key][tag_value].append(column["Name"])
            LOGGER.debug(f"TO REMOVE: {to_remove}")
            for tag_key, tag_config in to_remove.items():
                for tag_value, columns in tag_config.items():
                    resource = {
                        "TableWithColumns": {
                            "DatabaseName": self._database,
                            "Name": self._table,
                            "ColumnNames": columns,
                        }
                    }
                    response = self._lf_client.remove_lf_tags_from_resource(
                        Resource=resource, LFTags=[{"TagKey": tag_key, "TagValues": [tag_value]}]
                    )
                    self._parse_and_log_lf_response(response, columns, {tag_key: tag_value}, "remove")

    def _apply_lf_tags_table(
        self, table_resource: ResourceTypeDef, existing_lf_tags: GetResourceLFTagsResponseTypeDef
    ) -> None:
        lf_tags_table = existing_lf_tags.get("LFTagsOnTable", [])
        LOGGER.debug(f"EXISTING TABLE TAGS: {lf_tags_table}")
        LOGGER.debug(f"CONFIG TAGS: {self._lf_tags}")

        to_remove = {
            tag["TagKey"]: tag["TagValues"]
            for tag in lf_tags_table
            if tag["TagKey"] not in self.lf_tags  # type: ignore
        }
        LOGGER.debug(f"TAGS TO REMOVE: {to_remove}")
        if to_remove:
            response = self._lf_client.remove_lf_tags_from_resource(
                Resource=table_resource, LFTags=[{"TagKey": k, "TagValues": v} for k, v in to_remove.items()]
            )
            self._parse_and_log_lf_response(response, None, self._lf_tags, "remove")

        if self._lf_tags:
            response = self._lf_client.add_lf_tags_to_resource(
                Resource=table_resource, LFTags=[{"TagKey": k, "TagValues": [v]} for k, v in self._lf_tags.items()]
            )
            self._parse_and_log_lf_response(response, None, self._lf_tags)

    def _apply_lf_tags_columns(self) -> None:
        if self._lf_tags_columns:
            for tag_key, tag_config in self._lf_tags_columns.items():
                for tag_value, columns in tag_config.items():
                    resource = {
                        "TableWithColumns": {
                            "DatabaseName": self._database,
                            "Name": self._table,
                            "ColumnNames": columns,
                        }
                    }
                    response = self._lf_client.add_lf_tags_to_resource(
                        Resource=resource,
                        LFTags=[{"TagKey": tag_key, "TagValues": [tag_value]}],
                    )
                    self._parse_and_log_lf_response(response, columns, {tag_key: tag_value})

    def _parse_and_log_lf_response(
        self,
        response: Union[AddLFTagsToResourceResponseTypeDef, RemoveLFTagsFromResourceResponseTypeDef],
        columns: Optional[List[str]] = None,
        lf_tags: Optional[Dict[str, str]] = None,
        verb: str = "add",
    ) -> None:
        table_appendix = f".{self._table}" if self._table else ""
        columns_appendix = f" for columns {columns}" if columns else ""
        resource_msg = self._database + table_appendix + columns_appendix
        if failures := response.get("Failures", []):
            base_msg = f"Failed to {verb} LF tags: {lf_tags} to " + resource_msg
            for failure in failures:
                tag = failure.get("LFTag", {}).get("TagKey")
                error = failure.get("Error", {}).get("ErrorMessage")
                LOGGER.error(f"Failed to {verb} {tag} for " + resource_msg + f" - {error}")
            raise DbtRuntimeError(base_msg)
        LOGGER.debug(f"Success: {verb} LF tags {lf_tags} to " + resource_msg)


lf_tags_manager = LfTagsManager()
