from .base import PropertiesBaseSchema, FeatureBaseSchema, GeoJsonBaseSchema
from marshmallow import fields

class PropertiesGroupedByDaySchema(PropertiesBaseSchema):
    interval = fields.Integer(
        default=24,
        validate=lambda n: n >= 1,
        error_messages={"validator_failed": "interval must be greater than or equal to 1"},
    )


class FeatureGroupedByDaySchema(FeatureBaseSchema):
    properties = fields.Nested(
        PropertiesGroupedByDaySchema,
        missing=dict,
    )


class GeoJsonGroupedByDaySchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FeatureGroupedByDaySchema),
        required=True,
    )

class PropertiesGroupedByIntervalSchema(PropertiesBaseSchema):
    interval = fields.Integer(
        default=24,
        validate=lambda n: n >= 1,
        error_messages={"validator_failed": "interval must be greater than or equal to 1"},
    )


class FeatureGroupedByIntervalSchema(FeatureBaseSchema):
    properties = fields.Nested(
        PropertiesGroupedByIntervalSchema,
        missing=dict,
    )


class GeoJsonGroupedByIntervalSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FeatureGroupedByIntervalSchema),
        required=True,
    )
    

#GeoJsonWithHeadersSave