from .base import CallbackInfoSchema
from .counts import FrequencySchema, DeviceCountByDaySchema, DeviceCountByHourSchema
from marshmallow import Schema, fields, validate

class ZipCodeSchema(Schema):
    zipcode = fields.Str(required=True, validate=validate.Length(min=1))
    count = fields.Int(required=True, validate=validate.Range(min=0))


class ZipCodesSchema(Schema):
    total = fields.Int(required=True, validate=validate.Range(min=0))
    zipcodes = fields.List(fields.Nested(ZipCodeSchema), required=True)

class AttirbutionDataSchema(Schema):
    count = fields.Int(required=True, validate=validate.Range(min=0))
    frequency = fields.Nested(FrequencySchema, required=True)
    dayOfWeek = fields.List(fields.Nested(DeviceCountByDaySchema), required=True)
    hourOfDay = fields.List(fields.Nested(DeviceCountByHourSchema), required=True)
    zipcodes = fields.Nested(ZipCodesSchema, required=True)
    cities = fields.List(fields.Str(validate=validate.Length(min=1)), required=True)
    

class AttributionCallbackSchema(CallbackInfoSchema):
    data = fields.Nested(AttirbutionDataSchema, required=True)