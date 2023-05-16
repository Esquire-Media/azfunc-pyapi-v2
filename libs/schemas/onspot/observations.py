from .base import (
    FeatureBaseSchema,
    GeoJsonBaseSchema,
    PropertiesWithSaveSchema
)
from .options import ObservationOptions
from marshmallow import fields


class PropertiesObservationsWithSaveSchema(PropertiesWithSaveSchema):
    headers = fields.List(
        fields.Str(validate=fields.validate.OneOf(ObservationOptions)),
        default=ObservationOptions,
    )


class FeatureObservationsWithSaveSchema(FeatureBaseSchema):
    properties = fields.Nested(
        PropertiesObservationsWithSaveSchema(),
        required=True,
    )


class GeoJsonObservationsWithSaveSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FeatureObservationsWithSaveSchema()),
        required=True,
    )