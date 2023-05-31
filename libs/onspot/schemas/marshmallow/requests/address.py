from .base import WithSaveSchema, PropertiesBaseSchema
from marshmallow import Schema, fields, pre_load


class AddressMappingsSchema(Schema):
    street = fields.List(fields.String(), required=True)
    city = fields.List(fields.String(), required=True)
    state = fields.List(fields.String(), required=True)
    zip = fields.List(fields.String(), required=True)
    zip4 = fields.List(fields.String(), required=True)


class AddressWithSaveSchema(PropertiesBaseSchema, WithSaveSchema):
    sources = fields.List(
        fields.Url(schemes=["http", "https", "s3", "gs", "az"]), required=True
    )
    mappings = fields.Nested(AddressMappingsSchema, required=True)
    matchAcceptanceThreshold = fields.Float(default=29.9)
