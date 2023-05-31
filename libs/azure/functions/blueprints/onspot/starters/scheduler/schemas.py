from datetime import timedelta
from libs.data.key_value.table.azure_table.schemas.marshmallow import (
    AzureTableEntitySchema,
)
from libs.onspot import ENDPOINT_PATHS
from marshmallow import Schema, fields, validate


class JobSchema(AzureTableEntitySchema):
    Name = fields.String(required=True)
    Schedule = fields.TimeDelta(missing=lambda: timedelta(days=1))
    EndPoint = fields.String(required=True, validate=validate.OneOf(ENDPOINT_PATHS))


class RunSchema(AzureTableEntitySchema):
    JobKey = fields.String(required=True)

class ConfigSchema(RunSchema):
    Type = fields.String(required=True, validate=validate.OneOf(["Context", "Feature"]))
    Name = fields.String(required=True)
    Value = fields.String(required=True)
    
class ContextSchema():
    pass