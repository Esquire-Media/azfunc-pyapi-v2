from .base import FeatureBaseSchema, GeoJsonBaseSchema, PropertiesWithSaveSchema, FileBaseSchema
from marshmallow import fields


class PropertiesSocialsWithSaveSchema(PropertiesWithSaveSchema):
    countMin = fields.Int(
        validate=lambda n: n >= 5,
        error_messages={
            "validator_failed": "interval must be greater than or equal to 5"
        },
    )
    levels = fields.Int(validate=fields.validate.OneOf([1,2]))


class FeatureSocialsWithSaveSchema(FeatureBaseSchema):
    properties = fields.Nested(
        PropertiesSocialsWithSaveSchema,
        missing=dict,
    )


class GeoJsonSocialsWithSaveSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FeatureSocialsWithSaveSchema),
        required=True,
    )


class FileSocialsWithSaveSchema(FileBaseSchema):
    properties = fields.Nested(
        PropertiesSocialsWithSaveSchema,
        missing=dict,
    )


class FilesSocialsWithSaveSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FileSocialsWithSaveSchema),
        required=True,
    )