from .base import (
    PropertiesBaseSchema,
    FeatureBaseSchema,
    GeoJsonBaseSchema,
)
from .min_max import (
    FilesWithMinMaxSaveSchema,
    FileWithMinMaxSaveSchema,
    PropertiesWithMinMaxSaveSchema,
)
from .options import PoliticalAggregateOptions
from marshmallow import fields


class PropertiesPoliticalAggregateSchema(PropertiesBaseSchema):
    headers = fields.List(
        fields.Str(validate=fields.validate.OneOf(PoliticalAggregateOptions)),
        default=PoliticalAggregateOptions,
    )


class FeaturePoliticalAggregateSchema(FeatureBaseSchema):
    properties = fields.Nested(
        PropertiesPoliticalAggregateSchema(),
        required=True,
    )


class GeoJsonPoliticalAggregateSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FeaturePoliticalAggregateSchema()),
        required=True,
    )


class PropertiesPoliticalAggregateSchema(PropertiesBaseSchema):
    headers = fields.List(
        fields.Str(validate=fields.validate.OneOf(PoliticalAggregateOptions)),
        default=PoliticalAggregateOptions,
    )


class FeaturePoliticalWithSaveSchema(FeatureBaseSchema):
    properties = fields.Nested(
        PropertiesPoliticalAggregateSchema(),
        required=True,
    )


class GeoJsonPoliticalWithSaveSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FeaturePoliticalAggregateSchema()),
        required=True,
    )


class PropertiesPoliticalFileSchema(
    PropertiesWithMinMaxSaveSchema, PropertiesPoliticalAggregateSchema
):
    pass


class FilePoliticalWithSaveSchema(FileWithMinMaxSaveSchema):
    properties = fields.Nested(
        PropertiesPoliticalFileSchema(),
        required=True,
    )


class FilesPoliticalWithSaveSchema(FilesWithMinMaxSaveSchema):
    features = fields.List(
        fields.Nested(FilePoliticalWithSaveSchema()),
        required=True,
    )
