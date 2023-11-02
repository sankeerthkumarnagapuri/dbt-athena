from dataclasses import dataclass
from typing import Optional

from dbt.contracts.connection import AdapterResponse


@dataclass
class AthenaAdapterResponse(AdapterResponse):
    data_scanned_in_bytes: Optional[int] = None
