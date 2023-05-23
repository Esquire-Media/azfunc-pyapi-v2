from .base import GeoJsonBaseSchema, WithSaveSchema, PropertiesBaseSchema
from marshmallow import fields


class AttributionSchema(PropertiesBaseSchema):
    attributionFeatureCollection = fields.Nested(GeoJsonBaseSchema, required=True)
    targetingFeatureCollection = fields.Nested(GeoJsonBaseSchema, required=True)
    organization = fields.Str(required=False)


class AttributionWithSaveSchema(AttributionSchema, WithSaveSchema):
    pass
