from .base import GeoJsonBaseSchema, WithSaveSchema
from marshmallow import Schema, fields


class AttributionSchema(Schema):
    attributionFeatureCollection = GeoJsonBaseSchema(required=True)
    callback = fields.Url(required=True)
    name = fields.Str(required=True)
    targetingFeatureCollection = GeoJsonBaseSchema(required=True)
    organization = fields.Str(required=False)


class AttributionWithSaveSchema(AttributionSchema, WithSaveSchema):
    pass
