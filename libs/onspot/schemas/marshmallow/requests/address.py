from .base import WithSaveSchema, PropertiesBaseSchema
from marshmallow import Schema, fields, pre_load


class AddressMappingsSchema(Schema):
    """
    Schema for address mappings.

    This schema defines the structure for address mappings.

    Examples
    --------
    >>> schema = AddressMappingsSchema()
    >>> data = {"street": ["street1", "street2"], "city": ["city1", "city2"], "state": ["state1", "state2"], "zip": ["zip1", "zip2"], "zip4": ["zip4-1", "zip4-2"]}
    >>> schema.load(data)
    {'street': ['street1', 'street2'], 'city': ['city1', 'city2'], 'state': ['state1', 'state2'], 'zip': ['zip1', 'zip2'], 'zip4': ['zip4-1', 'zip4-2']}
    """
    street = fields.List(fields.String(), required=True)
    city = fields.List(fields.String(), required=True)
    state = fields.List(fields.String(), required=True)
    zip = fields.List(fields.String(), required=True)
    zip4 = fields.List(fields.String(), required=True)


class AddressWithSaveSchema(PropertiesBaseSchema, WithSaveSchema):
    """
    Schema for address with save options.

    This schema extends the base properties schema with save options and additional address fields.

    Examples
    --------
    >>> schema = AddressWithSaveSchema()
    >>> data = {"name": "Example", "organization": "Example Org", "outputProvider": "s3", "outputLocation": "s3://bucket/data", "fileName": "data.csv", "fileFormat": {"delimiter": ",", "quoteEncapsulate": True}, "sources": ["http://example.com/address1.csv", "http://example.com/address2.csv"], "mappings": {"street": ["street1", "street2"], "city": ["city1", "city2"], "state": ["state1", "state2"], "zip": ["zip1", "zip2"], "zip4": ["zip4-1", "zip4-2"]}, "matchAcceptanceThreshold": 29.9}
    >>> schema.load(data)
    {'name': 'Example', 'organization': 'Example Org', 'outputProvider': 's3', 'outputLocation': 's3://bucket/data', 'fileName': 'data.csv', 'fileFormat': {'delimiter': ',', 'quoteEncapsulate': True}, 'sources': ['http://example.com/address1.csv', 'http://example.com/address2.csv'], 'mappings': {'street': ['street1', 'street2'], 'city': ['city1', 'city2'], 'state': ['state1', 'state2'], 'zip': ['zip1', 'zip2'], 'zip4': ['zip4-1', 'zip4-2']}, 'matchAcceptanceThreshold': 29.9}
    """
    sources = fields.List(
        fields.Url(schemes=["http", "https", "s3", "gs", "az"]), required=True
    )
    mappings = fields.Nested(AddressMappingsSchema, required=True)
    matchAcceptanceThreshold = fields.Float(default=29.9)
