from .base import FeatureBaseSchema, GeoJsonBaseSchema, PropertiesWithSaveSchema
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
