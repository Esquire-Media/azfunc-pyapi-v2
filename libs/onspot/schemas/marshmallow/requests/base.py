from datetime import datetime, timedelta
from marshmallow import (
    Schema,
    fields,
    validate,
    validates,
    ValidationError,
    pre_load,
)
from marshmallow_geojson import (
    PropertiesSchema,
    FeatureSchema,
    FeatureCollectionSchema,
)
import uuid


class MissingNestedSchema(Schema):
    """
    Schema for handling missing nested fields.

    This schema is used to handle missing nested fields by loading the missing field value when it's a callable.

    Examples
    --------
    >>> schema = MissingNestedSchema()
    >>> data = {}
    >>> schema.load(data)
    {'nested_field': 'default_value'}
    """

    @pre_load
    def load_missing_nested(self, data, **kwargs):
        for fieldname, field in self.fields.items():
            if (
                fieldname not in data
                and isinstance(field, fields.Nested)
                and callable(field.missing)
            ):
                data[fieldname] = field.schema.load(field.missing())
        return data


class PropertiesBaseSchema(PropertiesSchema):
    """
    Base schema for properties in GeoJSON features.

    This schema defines the base properties for GeoJSON features.

    Examples
    --------
    >>> schema = PropertiesBaseSchema()
    >>> data = {"name": "Example", "callback": "example_callback", "hash": True}
    >>> schema.load(data)
    {'name': 'Example', 'callback': 'example_callback', 'hash': True}
    """

    name = fields.Str(missing=lambda: str(uuid.uuid4()))
    callback = fields.Str(required=True)
    hash = fields.Bool(required=False)

    @pre_load
    def meta_base(self, data, **kwargs):
        if "callback" in self.context.keys():
            if callable(self.context["callback"]):
                data["callback"] = self.context["callback"](self, data)
            else:
                data["callback"] = self.context["callback"]
        if "hash" in self.context.keys():
            data["hash"] = self.context["hash"]

        return data


class PropertiesGeoJsonSchema(PropertiesBaseSchema):
    """
    Schema for properties in GeoJSON features with additional geo-specific properties.

    This schema extends the base properties schema with additional geo-specific properties.

    Examples
    --------
    >>> schema = PropertiesGeoJsonSchema()
    >>> data = {"name": "Example", "callback": "example_callback", "hash": True}
    >>> schema.load(data)
    {'name': 'Example', 'callback': 'example_callback', 'hash': True, 'start': '2022-01-01T00:00:00', 'end': '2022-01-31T23:59:59', 'validate': True}
    """

    start = fields.DateTime(
        missing=lambda: datetime.fromisoformat(
            (datetime.utcnow() - timedelta(days=8)).date().isoformat()
        ).isoformat()
    )
    end = fields.DateTime(
        missing=lambda: datetime.fromisoformat(
            (datetime.utcnow() - timedelta(days=6)).date().isoformat()
        ).isoformat()
    )
    validate = fields.Bool(default=True)

    @validates("end")
    def validate_end(self, value):
        """
        Validate the end date.

        This method validates that the end date is before 5 days ago.

        Parameters
        ----------
        value : datetime or str
            The end date to validate.

        Raises
        ------
        ValidationError
            If the end date is not before 5 days ago.

        Examples
        --------
        >>> schema = PropertiesGeoJsonSchema()
        >>> schema.validate_end("2022-01-10T00:00:00")
        ValidationError: End date must be before 5 days ago
        """
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if value >= datetime.fromisoformat(
            (datetime.utcnow() - timedelta(days=5)).date().isoformat()
        ):
            raise ValidationError("End date must be before 5 days ago")

    @validates("start")
    def validate_start(self, value):
        """
        Validate the start date.

        This method validates that the start date is after 1st of the month 1 year ago.

        Parameters
        ----------
        value : datetime or str
            The start date to validate.

        Raises
        ------
        ValidationError
            If the start date is not after 1st of the month 1 year ago.

        Examples
        --------
        >>> schema = PropertiesGeoJsonSchema()
        >>> schema.validate_start("2021-01-01T00:00:00")
        ValidationError: Start date must be after 1st of the month 1 year ago
        """
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if value <= datetime.fromisoformat(
            datetime(datetime.now().year - 1, datetime.now().month, 1)
            .date()
            .isoformat()
        ):
            raise ValidationError(
                "Start date must be after 1st of the month 1 year ago"
            )

    @pre_load
    def meta_geo(self, data, **kwargs):
        if "start" in self.context.keys():
            if isinstance(self.context["start"], str):
                data["start"] = self.context["start"]
            elif isinstance(self.context["start"], datetime):
                data["start"] = self.context["start"].isoformat()
            elif isinstance(self.context["start"], timedelta):
                data["start"] = datetime.fromisoformat(
                    (datetime.utcnow() + self.context["start"]).date().isoformat()
                ).isoformat()
        if "end" in self.context.keys():
            if isinstance(self.context["end"], str):
                data["end"] = self.context["end"]
            elif isinstance(self.context["end"], datetime):
                data["end"] = self.context["end"].isoformat()
            elif isinstance(self.context["end"], timedelta):
                data["end"] = datetime.fromisoformat(
                    (datetime.utcnow() + self.context["end"]).date().isoformat()
                ).isoformat()
        return data


class FeatureBaseSchema(FeatureSchema, MissingNestedSchema):
    """
    Base schema for GeoJSON features.

    This schema defines the base schema for GeoJSON features.

    Examples
    --------
    >>> schema = FeatureBaseSchema()
    >>> data = {"type": "Feature", "properties": {"name": "Example"}}
    >>> schema.load(data)
    {'type': 'Feature', 'properties': {'name': 'Example'}}
    """

    type = fields.Str(
        validate=validate.OneOf(
            choices=["Feature"],
            error="Invalid feature type",
        ),
        default="Feature",
        missing="Feature",
    )
    properties = fields.Nested(PropertiesGeoJsonSchema, missing=dict)


class GeoJsonBaseSchema(FeatureCollectionSchema):
    """
    Base schema for GeoJSON feature collections.

    This schema defines the base schema for GeoJSON feature collections.

    Examples
    --------
    >>> schema = GeoJsonBaseSchema()
    >>> data = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {"name": "Example"}}]}
    >>> schema.load(data)
    {'type': 'FeatureCollection', 'features': [{'type': 'Feature', 'properties': {'name': 'Example'}}]}
    """

    type = fields.Str(
        validate=validate.OneOf(
            choices=["FeatureCollection"],
            error="Invalid feature collection type",
        ),
        default="FeatureCollection",
        missing="FeatureCollection",
    )
    features = fields.List(
        fields.Nested(FeatureBaseSchema),
        required=True,
    )


class FileFormatSchema(Schema):
    """
    Schema for file formats.

    This schema defines the properties for file formats.

    Examples
    --------
    >>> schema = FileFormatSchema()
    >>> data = {"delimiter": ",", "quoteEncapsulate": True, "compressionType": "gzip"}
    >>> schema.load(data)
    {'delimiter': ',', 'quoteEncapsulate': True, 'compressionType': 'gzip'}
    """

    delimiter = fields.Str(missing=",")
    quoteEncapsulate = fields.Bool(missing=True)
    compressionType = fields.Str()


class WithSaveSchema(MissingNestedSchema):
    """
    Schema for data with save options.

    This schema defines the properties for data with save options.

    Examples
    --------
    >>> schema = WithSaveSchema()
    >>> data = {"organization": "Example Org", "outputProvider": "s3", "outputLocation": "s3://bucket/data", "fileName": "data.csv", "fileFormat": {"delimiter": ",", "quoteEncapsulate": True}}
    >>> schema.load(data)
    {'organization': 'Example Org', 'outputProvider': 's3', 'outputLocation': 's3://bucket/data', 'fileName': 'data.csv', 'fileFormat': {'delimiter': ',', 'quoteEncapsulate': True}}
    """

    organization = fields.Str(required=False)
    outputProvider = fields.Str(
        default="s3", validate=fields.validate.OneOf(["s3", "gs", "az"]), required=False
    )
    outputLocation = fields.Url(required=True, schemes=["s3", "gs", "az"])
    fileName = fields.Str(required=False)
    fileFormat = fields.Nested(FileFormatSchema, missing=dict)

    @pre_load
    def meta_withsave(self, data: dict, **kwargs):
        if "organization" in self.context.keys():
            data["organization"] = self.context["organization"]
        if "outputProvider" in self.context.keys():
            data["outputProvider"] = self.context["outputProvider"]
        if "outputLocation" in self.context.keys():
            data["outputLocation"] = self.context["outputLocation"]
        if "outputAzConnStr" in self.context.keys() and "outputLocation" in data.keys():
            from azure.storage.blob import (
                BlobClient,
                ContainerSasPermissions,
                generate_container_sas,
            )

            config = {
                c[0 : c.index("=")]: c[c.index("=") + 1 :]
                for c in self.context["outputAzConnStr"].split(";")
            }
            container_name = data["outputLocation"].split("/")[0]
            blob_name = "/".join(data["outputLocation"].split("/")[1:])
            data["outputLocation"] = (
                BlobClient.from_connection_string(
                    conn_str=self.context["outputAzConnStr"],
                    container_name=container_name,
                    blob_name=blob_name,
                ).url
                + "?"
                + generate_container_sas(
                    account_name=config["AccountName"],
                    account_key=config["AccountKey"],
                    container_name=container_name,
                    permission=ContainerSasPermissions(read=True, write=True),
                    expiry=datetime.utcnow() + timedelta(days=2),
                )
            )
            data["outputLocation"] = data["outputLocation"].replace("https://", "az://")
        return data


class PropertiesWithSaveSchema(PropertiesBaseSchema, WithSaveSchema):
    """
    Schema for properties in GeoJSON features with save options.

    This schema extends the base properties schema with save options.

    Examples
    --------
    >>> schema = PropertiesWithSaveSchema()
    >>> data = {"name": "Example", "callback": "example_callback", "hash": True, "organization": "Example Org", "outputProvider": "s3", "outputLocation": "s3://bucket/data", "fileName": "data.csv", "fileFormat": {"delimiter": ",", "quoteEncapsulate": True}}
    >>> schema.load(data)
    {'name': 'Example', 'callback': 'example_callback', 'hash': True, 'organization': 'Example Org', 'outputProvider': 's3', 'outputLocation': 's3://bucket/data', 'fileName': 'data.csv', 'fileFormat': {'delimiter': ',', 'quoteEncapsulate': True}}
    """

    pass


class PropertiesGeoJsonWithSaveSchema(
    PropertiesGeoJsonSchema, PropertiesWithSaveSchema
):
    """
    Schema for properties in GeoJSON features with save options.

    This schema extends the GeoJSON properties schema with save options.

    Examples
    --------
    >>> schema = PropertiesGeoJsonWithSaveSchema()
    >>> data = {"name": "Example", "callback": "example_callback", "hash": True, "organization": "Example Org", "outputProvider": "s3", "outputLocation": "s3://bucket/data", "fileName": "data.csv", "fileFormat": {"delimiter": ",", "quoteEncapsulate": True}}
    >>> schema.load(data)
    {'name': 'Example', 'callback': 'example_callback', 'hash': True, 'organization': 'Example Org', 'outputProvider': 's3', 'outputLocation': 's3://bucket/data', 'fileName': 'data.csv', 'fileFormat': {'delimiter': ',', 'quoteEncapsulate': True}}
    """

    pass


class FeatureWithSaveSchema(FeatureBaseSchema):
    """
    Schema for GeoJSON features with save options.

    This schema extends the base feature schema with save options.

    Examples
    --------
    >>> schema = FeatureWithSaveSchema()
    >>> data = {"type": "Feature", "properties": {"name": "Example", "organization": "Example Org", "outputProvider": "s3", "outputLocation": "s3://bucket/data", "fileName": "data.csv", "fileFormat": {"delimiter": ",", "quoteEncapsulate": True}}}
    >>> schema.load(data)
    {'type': 'Feature', 'properties': {'name': 'Example', 'organization': 'Example Org', 'outputProvider': 's3', 'outputLocation': 's3://bucket/data', 'fileName': 'data.csv', 'fileFormat': {'delimiter': ',', 'quoteEncapsulate': True}}}
    """

    properties = fields.Nested(
        PropertiesGeoJsonWithSaveSchema,
        missing=dict,
    )


class GeoJsonWithSaveSchema(GeoJsonBaseSchema):
    """
    Schema for GeoJSON feature collections with save options.

    This schema extends the base GeoJSON feature collection schema with save options.

    Examples
    --------
    >>> schema = GeoJsonWithSaveSchema()
    >>> data = {"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {"name": "Example", "organization": "Example Org", "outputProvider": "s3", "outputLocation": "s3://bucket/data", "fileName": "data.csv", "fileFormat": {"delimiter": ",", "quoteEncapsulate": True}}}]}

    >>> schema.load(data)
    {'type': 'FeatureCollection', 'features': [{'type': 'Feature', 'properties': {'name': 'Example', 'organization': 'Example Org', 'outputProvider': 's3', 'outputLocation': 's3://bucket/data', 'fileName': 'data.csv', 'fileFormat': {'delimiter': ',', 'quoteEncapsulate': True}}}]}
    """

    features = fields.List(
        fields.Nested(FeatureWithSaveSchema),
        required=True,
    )


class FileBaseSchema(FeatureSchema, MissingNestedSchema):
    """
    Base schema for files in GeoJSON features.

    This schema defines the base schema for files in GeoJSON features.

    Examples
    --------
    >>> schema = FileBaseSchema()
    >>> data = {"type": "Files", "properties": {"name": "Example"}, "paths": ["http://example.com/file1.csv", "http://example.com/file2.csv"]}
    >>> schema.load(data)
    {'type': 'Files', 'properties': {'name': 'Example'}, 'paths': ['http://example.com/file1.csv', 'http://example.com/file2.csv']}
    """

    type = fields.Str(
        validate=fields.validate.OneOf(
            ["Files"],
            error="Invalid feature type",
        ),
        default="Files",
        missing="Files",
    )
    properties = fields.Nested(
        PropertiesBaseSchema,
        missing=dict,
    )
    paths = fields.List(
        fields.Url(schemes=["http", "https", "s3", "gs", "az"]), required=True
    )


class FilesBaseSchema(FeatureCollectionSchema):
    """
    Base schema for collections of files in GeoJSON format.

    This schema defines the base schema for collections of files in GeoJSON format.

    Examples
    --------
    >>> schema = FilesBaseSchema()
    >>> data = {"type": "FeatureCollection", "features": [{"type": "Files", "properties": {"name": "Example"}, "paths": ["http://example.com/file1.csv", "http://example.com/file2.csv"]}]
    >>> schema.load(data)
    {'type': 'FeatureCollection', 'features': [{'type': 'Files', 'properties': {'name': 'Example'}, 'paths': ['http://example.com/file1.csv', 'http://example.com/file2.csv']}]}
    """

    features = fields.List(
        fields.Nested(FileBaseSchema),
        required=True,
    )


class FileWithSaveSchema(FileBaseSchema):
    """
    Schema for files in GeoJSON features with save options.

    This schema extends the base file schema with save options.

    Examples
    --------
    >>> schema = FileWithSaveSchema()
    >>> data = {"type": "Files", "properties": {"name": "Example", "organization": "Example Org", "outputProvider": "s3", "outputLocation": "s3://bucket/data", "fileName": "data.csv", "fileFormat": {"delimiter": ",", "quoteEncapsulate": True}}, "paths": ["http://example.com/file1.csv", "http://example.com/file2.csv"]}
    >>> schema.load(data)
    {'type': 'Files', 'properties': {'name': 'Example', 'organization': 'Example Org', 'outputProvider': 's3', 'outputLocation': 's3://bucket/data', 'fileName': 'data.csv', 'fileFormat': {'delimiter': ',', 'quoteEncapsulate': True}}, 'paths': ['http://example.com/file1.csv', 'http://example.com/file2.csv']}
    """

    properties = fields.Nested(
        PropertiesWithSaveSchema,
        missing=dict,
    )


class FilesWithSaveSchema(GeoJsonBaseSchema):
    """
    Schema for collections of files in GeoJSON format with save options.

    This schema extends the base GeoJSON feature collection schema with save options.

    Examples
    --------
    >>> schema = FilesWithSaveSchema()
    >>> data = {"type": "FeatureCollection", "features": [{"type": "Files", "properties": {"name": "Example", "organization": "Example Org", "outputProvider": "s3", "outputLocation": "s3://bucket/data", "fileName": "data.csv", "fileFormat": {"delimiter": ",", "quoteEncapsulate": True}}, "paths": ["http://example.com/file1.csv", "http://example.com/file2.csv"]}]
    >>> schema.load(data)
    {'type': 'FeatureCollection', 'features': [{'type': 'Files', 'properties': {'name': 'Example', 'organization': 'Example Org', 'outputProvider': 's3', 'outputLocation': 's3://bucket/data', 'fileName': 'data.csv', 'fileFormat': {'delimiter': ',', 'quoteEncapsulate': True}}, 'paths': ['http://example.com/file1.csv', 'http://example.com/file2.csv']}]}
    """

    features = fields.List(
        fields.Nested(FileWithSaveSchema),
        required=True,
    )
