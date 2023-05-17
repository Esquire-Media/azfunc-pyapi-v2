from .base import CallbackInfoSchema, Coordinate
from marshmallow import fields

class DevicesCallbackSchema(CallbackInfoSchema):
    devices = fields.List(fields.String, required=True)
    
class DeviceLocationsCallbackSchema(CallbackInfoSchema):
    deviceLocations = fields.List(Coordinate, required=True)