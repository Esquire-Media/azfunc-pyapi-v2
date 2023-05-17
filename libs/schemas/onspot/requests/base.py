from datetime import datetime, timedelta
from marshmallow import Schema, fields, validates, ValidationError
from marshmallow_geojson import (
    PropertiesSchema,
    FeatureSchema,
    FeatureCollectionSchema,
)


class PropertiesBaseSchema(PropertiesSchema):
    name = fields.Str(required=True)
    callback = fields.Str(required=True)

class PropertiesGeoJsonSchema(PropertiesBaseSchema):
    start = fields.DateTime(required=True)
    end = fields.DateTime(required=True)
    validate = fields.Bool(default=True)

    @validates("end")
    def validate_end(self, value):
        # Check if the date is before 5 days ago
        if value >= datetime.utcnow() - timedelta(days=5):
            raise ValidationError("End date must be before 5 days ago")

    @validates("start")
    def validate_start(self, value):
        # Check if the date is after 1st of the month 1 year ago
        if value <= datetime(datetime.now().year - 1, datetime.now().month, 1):
            raise ValidationError(
                "Start date must be after 1st of the month 1 year ago"
            )

class FeatureBaseSchema(FeatureSchema):
    properties = fields.Nested(
        PropertiesGeoJsonSchema(),
        required=True,
    )


class GeoJsonBaseSchema(FeatureCollectionSchema):
    features = fields.List(
        fields.Nested(FeatureBaseSchema()),
        required=True,
    )


class FileFormatSchema(Schema):
    delimiter = fields.Str()
    quoteEncapsulate = fields.Bool()
    compressionType = fields.Str()


class WithSaveSchema(Schema):
    organization = fields.Str(required=False)
    outputProvider = fields.Str(
        default="s3", validate=fields.validate.OneOf(["s3", "gs"])
    )
    outputLocation = fields.Str()
    fileName = fields.Str()
    fileFormat = fields.Nested(FileFormatSchema)

class PropertiesWithSaveSchema(PropertiesBaseSchema, WithSaveSchema):
    pass

class FileBaseSchema(FeatureSchema):
    type = fields.Str(
        required=True,
        validate=fields.validate.OneOf(
            ["Files"],
            error="Invalid feature type",
        ),
    )
    properties = fields.Nested(
        PropertiesBaseSchema(),
        required=True,
    )
    paths = fields.List(fields.Url(), required=True)
    
class FilesBaseSchema(FeatureCollectionSchema):
    features = fields.List(
        fields.Nested(FileBaseSchema()),
        required=True,
    )


class FileWithSaveSchema(FileBaseSchema):
    properties = fields.Nested(
        PropertiesWithSaveSchema(),
        required=True,
    )
    
class GeoJsonWithSaveSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FileWithSaveSchema()),
        required=True,
    )