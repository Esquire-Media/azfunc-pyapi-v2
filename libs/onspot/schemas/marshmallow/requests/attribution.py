from .base import GeoJsonBaseSchema, WithSaveSchema, PropertiesBaseSchema
from marshmallow import fields


class AttributionSchema(PropertiesBaseSchema):
    """
    Schema for attribution.

    This schema defines the structure for attribution.

    Examples
    --------
    >>> schema = AttributionSchema()
    >>> data = {"attributionFeatureCollection": {"type": "FeatureCollection", "features": []}, "targetingFeatureCollection": {"type": "FeatureCollection", "features": []}, "organization": "Example Org"}
    >>> schema.load(data)
    {'attributionFeatureCollection': {'type': 'FeatureCollection', 'features': []}, 'targetingFeatureCollection': {'type': 'FeatureCollection', 'features': []}, 'organization': 'Example Org'}
    """
    attributionFeatureCollection = fields.Nested(GeoJsonBaseSchema, required=True)
    targetingFeatureCollection = fields.Nested(GeoJsonBaseSchema, required=True)
    organization = fields.Str(required=False)


class AttributionWithSaveSchema(AttributionSchema, WithSaveSchema):
    """
    Schema for attribution with save options.

    This schema extends the attribution schema with save options.

    Examples
    --------
    >>> schema = AttributionWithSaveSchema()
    >>> data = {"name": "Example", "organization": "Example Org", "outputProvider": "s3", "outputLocation": "s3://bucket/data", "fileName": "data.csv", "fileFormat": {"delimiter": ",", "quoteEncapsulate": True}, "attributionFeatureCollection": {"type": "FeatureCollection", "features": []}, "targetingFeatureCollection": {"type": "FeatureCollection", "features": []}}
    >>> schema.load(data)
    {'name': 'Example', 'organization': 'Example Org', 'outputProvider': 's3', 'outputLocation': 's3://bucket/data', 'fileName': 'data.csv', 'fileFormat': {'delimiter': ',', 'quoteEncapsulate': True}, 'attributionFeatureCollection': {'type': 'FeatureCollection', 'features': []}, 'targetingFeatureCollection': {'type': 'FeatureCollection', 'features': []}}
    """
    pass
