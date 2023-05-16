from .base import FeatureBaseSchema, GeoJsonBaseSchema, PropertiesWithSaveSchema
from marshmallow import fields


class PropertiesTradesWithSaveSchema(PropertiesWithSaveSchema):
    radius = fields.Float()
    includeHouseholds = fields.Boolean()
    includeWorkplaces = fields.Boolean()


class FeatureTradesWithSaveSchema(FeatureBaseSchema):
    properties = fields.Nested(
        PropertiesTradesWithSaveSchema(),
        required=True,
    )


class GeoJsonTradesWithSaveSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FeatureTradesWithSaveSchema()),
        required=True,
    )
