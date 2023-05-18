from .base import CallbackInfoSchema
from datetime import datetime
from marshmallow import Schema, fields, validate, post_load


class ObservationSchema(Schema):
    did = fields.Str(
        required=True, validate=validate.Length(min=1)
    )  # Assuming did is a non-empty string
    timestamp = fields.Integer(
        required=True, validate=validate.Range(min=0)
    )  # Assuming timestamp is a non-negative integer

    @post_load
    def convert_timestamp(self, data, **kwargs):
        # Convert timestamp from milliseconds to seconds and then into a datetime object
        data["timestamp"] = datetime.fromtimestamp(data["timestamp"] / 1000.0)
        return data

class ObservationsCallbackSchema(CallbackInfoSchema):
    observations = fields.List(fields.Nested(ObservationSchema), required=True)