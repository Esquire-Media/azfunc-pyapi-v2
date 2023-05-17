from .base import WithSaveSchema
from marshmallow import Schema, fields


class AddressMappingsSchema(Schema):
    street = fields.List(fields.String(), required=True)
    city = fields.List(fields.String(), required=True)
    state = fields.List(fields.String(), required=True)
    zip = fields.List(fields.String(), required=True)
    zip4 = fields.List(fields.String(), required=True)


class AddressWithSaveSchema(WithSaveSchema):
    name = fields.String(required=True)
    sources = fields.List(fields.Url(), required=True)
    mappings = fields.Nested(AddressMappingsSchema, required=True)
    callback = fields.Url(required=True)
    matchAcceptanceThreshold = fields.Float(default=29.9)
