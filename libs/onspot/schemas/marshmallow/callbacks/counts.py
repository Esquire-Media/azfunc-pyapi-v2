from .base import CallbackInfoSchema
from marshmallow import Schema, fields, validate


class CountCallbackSchema(CallbackInfoSchema):
    count = fields.Int()


class DeviceCountByDaySchema(Schema):
    count = fields.Integer(required=True, validate=validate.Range(min=0))
    day = fields.Str(
        required=True,
        validate=validate.OneOf(
            [
                "SUNDAY",
                "MONDAY",
                "TUESDAY",
                "WEDNESDAY",
                "THURSDAY",
                "FRIDAY",
                "SATURDAY",
            ]
        ),
    )


class CountByDayCallbackSchema(CallbackInfoSchema):
    deviceCountByDay = fields.List(fields.Nested(DeviceCountByDaySchema), required=True)


class DeviceCountByDIDSchema(Schema):
    count = fields.Integer(required=True, validate=validate.Range(min=0))
    did = fields.Str(required=True)


class CountByDeviceCallbackSchema(CallbackInfoSchema):
    deviceCountByDevice = fields.List(
        fields.Nested(DeviceCountByDIDSchema), required=True
    )


class DeviceCountByHourSchema(Schema):
    count = fields.Integer(required=True, validate=validate.Range(min=0))
    hour = fields.Str(
        required=True, validate=validate.OneOf([str(i) for i in range(24)])
    )


class CountByHourCallbackSchema(CallbackInfoSchema):
    deviceCountByHour = fields.List(
        fields.Nested(DeviceCountByHourSchema), required=True
    )


class DeviceCountByIntervalSchema(Schema):
    start = fields.DateTime(required=True)
    end = fields.DateTime(required=True)
    count = fields.Integer(required=True, validate=validate.Range(min=0))


class CountByIntervalCallbackSchema(CallbackInfoSchema):
    deviceCountByInterval = fields.List(
        fields.Nested(DeviceCountByIntervalSchema), required=True
    )

class FrequencySchema(Schema):
    one = fields.Integer(required=True, validate=validate.Range(min=0))
    two = fields.Integer(required=True, validate=validate.Range(min=0))
    three = fields.Integer(required=True, validate=validate.Range(min=0))
    fourToSeven = fields.Integer(required=True, validate=validate.Range(min=0))
    eightToFifteen = fields.Integer(required=True, validate=validate.Range(min=0))
    sixteenToTwentyfive = fields.Integer(required=True, validate=validate.Range(min=0))
    twentySixOrMore = fields.Integer(required=True, validate=validate.Range(min=0))

class CountByFrequencyCallbackSchema(CallbackInfoSchema, FrequencySchema):
    pass