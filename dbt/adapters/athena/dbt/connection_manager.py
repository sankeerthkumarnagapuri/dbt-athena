import json
import re
from contextlib import contextmanager
from typing import ContextManager, Tuple

from pyathena.connection import Connection as AthenaConnection
from pyathena.model import AthenaQueryExecution
from pyathena.util import RetryConfig

from dbt.adapters.athena.aws.config import get_boto3_config
from dbt.adapters.athena.aws.session import get_boto3_session
from dbt.adapters.athena.constants import LOGGER
from dbt.adapters.athena.dbt.adapter_response import AthenaAdapterResponse
from dbt.adapters.athena.dbt.credentials import AthenaCredentials
from dbt.adapters.athena.pyathena.cursor import AthenaCursor
from dbt.adapters.athena.pyathena.formatter import AthenaParameterFormatter
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection, ConnectionState
from dbt.exceptions import ConnectionError, DbtRuntimeError


class AthenaConnectionManager(SQLConnectionManager):
    TYPE = "athena"

    @classmethod
    def data_type_code_to_name(cls, type_code: str) -> str:
        """
        Get the string representation of the data type from the Athena metadata. Dbt performs a
        query to retrieve the types of the columns in the SQL query. Then these types are compared
        to the types in the contract config, simplified because they need to match what is returned
        by Athena metadata (we are only interested in the broader type, without subtypes nor granularity).
        """
        return type_code.split("(")[0].split("<")[0].upper()

    @contextmanager  # type: ignore
    def exception_handler(self, sql: str) -> ContextManager:  # type: ignore
        try:
            yield
        except Exception as e:
            LOGGER.debug(f"Error running SQL: {sql}")
            raise DbtRuntimeError(str(e)) from e

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == "open":
            LOGGER.debug("Connection is already open, skipping open.")
            return connection

        try:
            creds: AthenaCredentials = connection.credentials

            handle = AthenaConnection(
                s3_staging_dir=creds.s3_staging_dir,
                endpoint_url=creds.endpoint_url,
                catalog_name=creds.database,
                schema_name=creds.schema,
                work_group=creds.work_group,
                cursor_class=AthenaCursor,
                cursor_kwargs={"debug_query_state": creds.debug_query_state},
                formatter=AthenaParameterFormatter(),
                poll_interval=creds.poll_interval,
                session=get_boto3_session(connection),
                retry_config=RetryConfig(
                    attempt=creds.num_retries + 1,
                    exceptions=("ThrottlingException", "TooManyRequestsException", "InternalServerException"),
                ),
                config=get_boto3_config(),
            )

            connection.state = ConnectionState.OPEN
            connection.handle = handle

        except Exception as exc:
            LOGGER.exception(f"Got an error when attempting to open a Athena connection due to {exc}")
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise ConnectionError(str(exc))

        return connection

    @classmethod
    def get_response(cls, cursor: AthenaCursor) -> AthenaAdapterResponse:
        code = "OK" if cursor.state == AthenaQueryExecution.STATE_SUCCEEDED else "ERROR"
        rowcount, data_scanned_in_bytes = cls.process_query_stats(cursor)
        return AthenaAdapterResponse(
            _message=f"{code} {rowcount}",
            rows_affected=rowcount,
            code=code,
            data_scanned_in_bytes=data_scanned_in_bytes,
        )

    @staticmethod
    def process_query_stats(cursor: AthenaCursor) -> Tuple[int, int]:
        """
        Helper function to parse query statistics from SELECT statements.
        The function looks for all statements that contains rowcount or data_scanned_in_bytes,
        then strip the SELECT statements, and pick the value between curly brackets.
        """
        if all(map(cursor.query.__contains__, ["rowcount", "data_scanned_in_bytes"])):
            try:
                query_split = cursor.query.lower().split("select")[-1]
                # query statistics are in the format {"rowcount":1, "data_scanned_in_bytes": 3}
                # the following statement extract the content between { and }
                query_stats = re.search("{(.*)}", query_split)
                if query_stats:
                    stats = json.loads("{" + query_stats.group(1) + "}")
                    return stats.get("rowcount", -1), stats.get("data_scanned_in_bytes", 0)
            except Exception as err:
                LOGGER.debug(f"There was an error parsing query stats {err}")
                return -1, 0
        return cursor.rowcount, cursor.data_scanned_in_bytes

    def cancel(self, connection: Connection) -> None:
        pass

    def add_begin_query(self) -> None:
        pass

    def add_commit_query(self) -> None:
        pass

    def begin(self) -> None:
        pass

    def commit(self) -> None:
        pass
