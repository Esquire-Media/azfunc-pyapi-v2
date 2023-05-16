from .base import (
    FeatureBaseSchema,
    GeoJsonBaseSchema,
)
from .min_max import PropertiesWithMinMax
from .options import DemographicsOptions
from marshmallow import fields


class PropertiesDemographicsSchema(PropertiesWithMinMax):
    demographics = fields.List(
        fields.Str(validate=fields.validate.OneOf(DemographicsOptions)),
        default=DemographicsOptions,
    )


class FeatureDemographicsSchema(FeatureBaseSchema):
    properties = fields.Nested(
        PropertiesDemographicsSchema(),
        required=True,
    )


class GeoJsonDemographicsSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FeatureDemographicsSchema()),
        required=True,
    )