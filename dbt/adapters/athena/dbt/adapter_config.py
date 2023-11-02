from dataclasses import dataclass
from typing import Any, Dict, Optional

from dbt.adapters.protocol import AdapterConfig


@dataclass
class AthenaConfig(AdapterConfig):
    """
    Database and relation-level configs.

    Args:
        work_group: Identifier of Athena workgroup.
        s3_staging_dir: S3 location to store Athena query results and metadata.
        external_location: If set, the full S3 path in which the table will be saved.
        partitioned_by: An array list of columns by which the table will be partitioned.
        bucketed_by: An array list of columns to bucket data, ignored if using Iceberg.
        bucket_count: The number of buckets for bucketing your data, ignored if using Iceberg.
        table_type: The type of table, supports hive or iceberg.
        ha: If the table should be built using the high-availability method.
        format: The data format for the table. Supports ORC, PARQUET, AVRO, JSON, TEXTFILE.
        write_compression: The compression type to use for any storage format
            that allows compression to be specified.
        field_delimiter: Custom field delimiter, for when format is set to TEXTFILE.
        table_properties : Table properties to add to the table, valid for Iceberg only.
        native_drop:  Relation drop operations will be performed with SQL, not direct Glue API calls.
        seed_by_insert: default behaviour uploads seed data to S3.
        lf_tags_config: AWS lakeformation tags to associate with the table and columns.
        seed_s3_upload_args: Dictionary containing boto3 ExtraArgs when uploading to S3.
        partitions_limit: Maximum numbers of partitions when batching.
    """

    work_group: Optional[str] = None
    s3_staging_dir: Optional[str] = None
    external_location: Optional[str] = None
    partitioned_by: Optional[str] = None
    bucketed_by: Optional[str] = None
    bucket_count: Optional[str] = None
    table_type: str = "hive"
    ha: bool = False
    format: str = "parquet"
    write_compression: Optional[str] = None
    field_delimiter: Optional[str] = None
    table_properties: Optional[str] = None
    native_drop: Optional[str] = None
    seed_by_insert: bool = False
    lf_tags_config: Optional[Dict[str, Any]] = None
    seed_s3_upload_args: Optional[Dict[str, Any]] = None
    partitions_limit: Optional[int] = None
