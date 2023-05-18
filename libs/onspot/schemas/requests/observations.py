from .base import GeoJsonWithSaveSchema, FeatureWithSaveSchema, PropertiesWithSaveSchema
from marshmallow import fields

ObservationOptions = [
    "location",
    "deviceid",
    "timestamp",
    "date",
    "time",
    "dayofweek",
    "lat",
    "lng",
]


class PropertiesObservationsWithSaveSchema(PropertiesWithSaveSchema):
    headers = fields.List(
        fields.Str(validate=fields.validate.OneOf(ObservationOptions)),
        default=ObservationOptions,
    )


class FeatureObservationsWithSaveSchema(FeatureWithSaveSchema):
    properties = fields.Nested(
        PropertiesObservationsWithSaveSchema,
        missing=dict,
    )


class GeoJsonObservationsWithSaveSchema(GeoJsonWithSaveSchema):
    features = fields.List(
        fields.Nested(FeatureObservationsWithSaveSchema),
        required=True,
    )
