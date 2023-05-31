from .base import CallbackInfoSchema
from marshmallow import Schema, fields, validate

class ZipCodeField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        if not isinstance(value, dict):
            raise validate.ValidationError("Value must be a dictionary.")
        for key, count in value.items():
            if not isinstance(key, str):
                raise validate.ValidationError("Keys must be string.")
            if not isinstance(count, int) or count < 0:
                raise validate.ValidationError("Values must be non-negative integers.")
        return value
    
class DemographicZipcodeSchema(Schema):
    zipcode = ZipCodeField(required=True)

class DemographicZipcodesCallbackSchema(CallbackInfoSchema):
    totalMatched = fields.Integer(required=True, validate=validate.Range(min=0))
    demographics = fields.Nested(DemographicZipcodeSchema, required=True)
    