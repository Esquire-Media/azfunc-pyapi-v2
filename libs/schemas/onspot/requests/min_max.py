from .base import (
    PropertiesBaseSchema,
    FeatureBaseSchema,
    GeoJsonBaseSchema,
    WithSaveSchema,
    FileBaseSchema,
)
from marshmallow import Schema, fields, validates, ValidationError


class MinMaxSchema(Schema):
    min = fields.Integer(
        default=1,
        validate=lambda n: n >= 1,
        error_messages={"validator_failed": "min must be greater than or equal to 1"},
    )
    max = fields.Integer(default=20)

    @validates("max")
    def validate_max(self, value):
        if self.context.get("min") is not None and value < self.context["min"]:
            raise ValidationError("max must be greater than or equal to min")

    @validates("min")
    def validate_min(self, value):
        if self.context.get("max") is not None and value > self.context["max"]:
            raise ValidationError("min must be less than or equal to max")


class PropertiesWithMinMax(PropertiesBaseSchema, MinMaxSchema):
    pass


class FeatureWithMinMaxSchema(FeatureBaseSchema):
    properties = fields.Nested(
        PropertiesWithMinMax(),
        required=True,
    )


class GeoJsonWithMinMaxSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FeatureWithMinMaxSchema()),
        required=True,
    )


class PropertiesWithMinMaxSaveSchema(
    PropertiesBaseSchema, MinMaxSchema, WithSaveSchema
):
    pass


class FeatureWithMinMaxSaveSchema(FeatureBaseSchema):
    properties = fields.Nested(
        PropertiesWithMinMaxSaveSchema(),
        required=True,
    )


class GeoJsonWithMinMaxSaveSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FeatureWithMinMaxSaveSchema()),
        required=True,
    )


class PropertiesFilesWithMinMaxSaveSchema(PropertiesWithMinMaxSaveSchema):
    includeNonMatchedDevices = fields.Boolean()


class FileWithMinMaxSaveSchema(FileBaseSchema):
    properties = fields.Nested(
        PropertiesFilesWithMinMaxSaveSchema(),
        required=True,
    )


class FilesWithMinMaxSaveSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FileWithMinMaxSaveSchema()),
        required=True,
    )
