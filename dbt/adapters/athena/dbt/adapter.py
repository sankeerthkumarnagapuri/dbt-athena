from typing import Any, Dict

import agate

from dbt.adapters.athena.aws.lakeformation.tags_manager import lf_tags_manager
from dbt.adapters.athena.constants import LOGGER
from dbt.adapters.athena.dbt.adapter_config import AthenaConfig
from dbt.adapters.athena.dbt.connection_manager import AthenaConnectionManager
from dbt.adapters.athena.dbt.relation import AthenaRelation
from dbt.adapters.base import ConstraintSupport, available
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.nodes import ConstraintType


class AthenaAdapter(SQLAdapter):
    BATCH_CREATE_PARTITION_API_LIMIT = 100
    BATCH_DELETE_PARTITION_API_LIMIT = 25

    ConnectionManager = AthenaConnectionManager
    Relation = AthenaRelation
    AdapterSpecificConfigs = AthenaConfig

    # There is no such concept as constraints in Athena
    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    @classmethod
    def date_function(cls) -> str:
        return "now()"

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "double" if decimals else "integer"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp"

    @available
    def add_lf_tags_to_database(self, relation: AthenaRelation) -> None:
        conn = self.connections.get_thread_connection()
        if lf_tags := conn.credentials.lf_tags_database:
            lf_tags_manager.process_lf_tags_database(
                conn.handle,
                relation,
                lf_tags,
            )
        else:
            LOGGER.debug(f"Lakeformation is disabled for {relation}")

    @available
    def add_lf_tags(self, relation: AthenaRelation, lf_tags_config: Dict[str, Any]) -> None:
        conn = self.connections.get_thread_connection()
        lf_tags_manager.process_lf_tags(
            conn.handle,
            relation,
            lf_tags_config,
        )
        LOGGER.debug(f"Lakeformation is disabled for {relation}")
