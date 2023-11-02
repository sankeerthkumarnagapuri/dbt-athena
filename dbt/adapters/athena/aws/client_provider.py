from asyncio import Lock
from typing import Generic, TypeVar

from pyathena.connection import Connection as AthenaConnection

from dbt.adapters.athena.aws.config import get_boto3_config

AWSServiceClient = TypeVar("AWSServiceClient")

boto3_client_lock = Lock()


class AWSClientProvider(Generic[AWSServiceClient]):
    def __init__(self, service_name: str):
        self._service_name = service_name

    def get_service_client(self, connection: AthenaConnection) -> AWSServiceClient:
        with boto3_client_lock:
            service_client = connection.session.client(
                self._service_name, connection.region_name, config=get_boto3_config()
            )
        return service_client
