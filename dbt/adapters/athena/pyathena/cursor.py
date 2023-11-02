import time
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, Dict, Optional

import tenacity
from pyathena.cursor import Cursor
from pyathena.error import OperationalError
from pyathena.model import AthenaQueryExecution
from pyathena.result_set import AthenaResultSet
from tenacity.retry import retry_if_exception
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_exponential

from dbt.adapters.athena.constants import LOGGER


class AthenaCursor(Cursor):
    def __init__(self, **kwargs) -> None:  # type: ignore
        super().__init__(**kwargs)
        self._executor = ThreadPoolExecutor()

    def _collect_result_set(self, query_id: str) -> AthenaResultSet:
        query_execution = self._poll(query_id)
        return self._result_set_class(
            connection=self._connection,
            converter=self._converter,
            query_execution=query_execution,
            arraysize=self._arraysize,
            retry_config=self._retry_config,
        )

    def _poll(self, query_id: str) -> AthenaQueryExecution:
        try:
            query_execution = self.__poll(query_id)
        except KeyboardInterrupt as e:
            if self._kill_on_interrupt:
                logger.warning("Query canceled by user.")
                self._cancel(query_id)
                query_execution = self.__poll(query_id)
            else:
                raise e
        return query_execution

    def __poll(self, query_id: str) -> AthenaQueryExecution:
        while True:
            query_execution = self._get_query_execution(query_id)
            if query_execution.state in [
                AthenaQueryExecution.STATE_SUCCEEDED,
                AthenaQueryExecution.STATE_FAILED,
                AthenaQueryExecution.STATE_CANCELLED,
            ]:
                return query_execution

            if self.connection.cursor_kwargs.get("debug_query_state", False):
                LOGGER.debug(f"Query state is: {query_execution.state}. Sleeping for {self._poll_interval}...")
            time.sleep(self._poll_interval)

    def execute(  # type: ignore
        self,
        operation: str,
        parameters: Optional[Dict[str, Any]] = None,
        work_group: Optional[str] = None,
        s3_staging_dir: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        cache_size: int = 0,
        cache_expiration_time: int = 0,
        catch_partitions_limit: bool = False,
        **kwargs,
    ):
        def inner() -> AthenaCursor:
            query_id = self._execute(
                operation,
                parameters=parameters,
                work_group=work_group,
                s3_staging_dir=s3_staging_dir,
                cache_size=cache_size,
                cache_expiration_time=cache_expiration_time,
            )
            query_execution = self._executor.submit(self._collect_result_set, query_id).result()
            if query_execution.state == AthenaQueryExecution.STATE_SUCCEEDED:
                self.result_set = self._result_set_class(
                    self._connection,
                    self._converter,
                    query_execution,
                    self.arraysize,
                    self._retry_config,
                )

            else:
                raise OperationalError(query_execution.state_change_reason)
            return self

        retry = tenacity.Retrying(
            # No need to retry if TOO_MANY_OPEN_PARTITIONS occurs.
            # Otherwise, Athena throws ICEBERG_FILESYSTEM_ERROR after retry,
            # because not all files are removed immediately after first try to create table
            retry=retry_if_exception(
                lambda e: False if catch_partitions_limit and "TOO_MANY_OPEN_PARTITIONS" in str(e) else True
            ),
            stop=stop_after_attempt(self._retry_config.attempt),
            wait=wait_exponential(
                multiplier=self._retry_config.attempt,
                max=self._retry_config.max_delay,
                exp_base=self._retry_config.exponential_base,
            ),
            reraise=True,
        )
        return retry(inner)
