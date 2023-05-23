from azure.data.tables import TableEntity
from marshmallow import Schema, fields, types
from typing import Mapping, Any, Iterable
import logging


class MetadataSchema(Schema):
    timestamp = fields.DateTime(data_key="timestamp")

class AzureTableEntitySchema(Schema):
    PartitionKey = fields.String(required=True)
    RowKey = fields.String(required=True)
    metadata = fields.Nested(MetadataSchema)

    # def load(
    #     self,
    #     data: Mapping[str, Any] | Iterable[Mapping[str, Any]] | TableEntity,
    #     *,
    #     many: bool | None = None,
    #     partial: types.StrSequenceOrSet | bool | None = None,
    #     unknown: str | None = None
    # ):
    #     data["Timestamp"] = data.metadata["timestamp"].isoformat()
    #     data["ETag"] = data.metadata["etag"]
    #     return super().load(data, many=many, partial=partial, unknown=unknown)
