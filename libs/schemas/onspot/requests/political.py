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
from marshmallow import fields


PoliticalAggregateOptions = [
    "congressional_district",
    "state_senate_district",
    "state_assembly_district",
    "conservative_party_probability",
    "democratic_party_probability",
    "green_party_probability",
    "independent_party_probability",
    "libertarian_party_probability",
    "liberal_party_probability",
    "republican_party_probability",
    "state_donation_amount_percentile",
    "state_donation_amount_prediction",
    "state_donor_probability",
    "federal_donation_amount_percentile",
    "federal_donation_amount_prediction",
    "federal_donor_probability",
    "turnout_probability_midterm_general",
    "turnout_probability_midterm_primary",
    "turnout_probability_presidential_general",
    "turnout_probability_presidential_primary",
]


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


class PropertiesPoliticalFileBaseSchema(
    PropertiesWithMinMaxSaveSchema, PropertiesPoliticalAggregateSchema
):
    pass


class FilePoliticalWithSaveSchema(FileWithMinMaxSaveSchema):
    properties = fields.Nested(
        PropertiesPoliticalFileBaseSchema(),
        required=True,
    )


class FilesPoliticalWithSaveSchema(FilesWithMinMaxSaveSchema):
    features = fields.List(
        fields.Nested(FilePoliticalWithSaveSchema()),
        required=True,
    )
