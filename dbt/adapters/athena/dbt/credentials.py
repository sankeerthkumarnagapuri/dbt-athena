import hashlib
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from dbt.contracts.connection import Credentials


@dataclass
class AthenaCredentials(Credentials):
    s3_staging_dir: str
    region_name: str
    endpoint_url: Optional[str] = None
    work_group: Optional[str] = None
    aws_profile_name: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    poll_interval: float = 1.0
    debug_query_state: bool = False
    _ALIASES = {"catalog": "database"}
    num_retries: int = 5
    s3_data_dir: Optional[str] = None
    s3_data_naming: Optional[str] = "schema_table_unique"
    s3_tmp_table_dir: Optional[str] = None
    # Unfortunately we can not just use dict, must be Dict because we'll get the following error:
    # Credentials in profile "athena", target "athena" invalid: Unable to create schema for 'dict'
    seed_s3_upload_args: Optional[Dict[str, Any]] = None
    lf_tags_database: Optional[Dict[str, str]] = None

    @property
    def type(self) -> str:
        return "athena"

    @property
    def unique_field(self) -> str:
        return f"athena-{hashlib.md5(self.s3_staging_dir.encode()).hexdigest()}"

    def _connection_keys(self) -> Tuple[str, ...]:
        return (
            "s3_staging_dir",
            "work_group",
            "region_name",
            "database",
            "schema",
            "poll_interval",
            "aws_profile_name",
            "aws_access_key_id",
            "aws_secret_access_key",
            "endpoint_url",
            "s3_data_dir",
            "s3_data_naming",
            "s3_tmp_table_dir",
            "debug_query_state",
            "seed_s3_upload_args",
            "lf_tags_database",
        )
