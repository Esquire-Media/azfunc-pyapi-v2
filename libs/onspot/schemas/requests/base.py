from datetime import datetime, timedelta
from marshmallow import (
    Schema,
    fields,
    validate,
    validates,
    ValidationError,
    pre_load,
    post_load,
)
from marshmallow_geojson import (
    PropertiesSchema,
    FeatureSchema,
    FeatureCollectionSchema,
)
import uuid
import logging


class MissingNestedSchema(Schema):
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
        # Check if the date is before 5 days ago
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if value >= datetime.fromisoformat(
            (datetime.utcnow() - timedelta(days=5)).date().isoformat()
        ):
            raise ValidationError("End date must be before 5 days ago")

    @validates("start")
    def validate_start(self, value):
        # Check if the date is after 1st of the month 1 year ago
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
    delimiter = fields.Str(missing=",")
    quoteEncapsulate = fields.Bool(missing=True)
    compressionType = fields.Str()


class WithSaveSchema(MissingNestedSchema):
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

    @post_load
    def meta_withsave_filename(self, data: dict, **kwargs):
        if not data.get("fileName"):
            data["fileName"] = data["name"]
        if "prefix_fileName" in self.context.keys():
            if callable(self.context["prefix_fileName"]):
                data["fileName"] = self.context["prefix_fileName"](self, data) + data.get("fileName", "")
            else:
                data["fileName"] = self.context["prefix_fileName"] + data.get("fileName", "")
        if "suffix_fileName" in self.context.keys():
            if callable(self.context["suffix_fileName"]):
                data["fileName"] =  data.get("fileName", "") + self.context["suffix_fileName"](self, data)
            else:
                data["fileName"] = data.get("fileName", "") + self.context["suffix_fileName"]
        return data


class PropertiesWithSaveSchema(PropertiesBaseSchema, WithSaveSchema):
    pass


class PropertiesGeoJsonWithSaveSchema(
    PropertiesGeoJsonSchema, PropertiesWithSaveSchema
):
    pass


class FeatureWithSaveSchema(FeatureBaseSchema):
    properties = fields.Nested(
        PropertiesGeoJsonWithSaveSchema,
        missing=dict,
    )


class GeoJsonWithSaveSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FeatureWithSaveSchema),
        required=True,
    )


class FileBaseSchema(FeatureSchema, MissingNestedSchema):
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
    features = fields.List(
        fields.Nested(FileBaseSchema),
        required=True,
    )


class FileWithSaveSchema(FileBaseSchema):
    properties = fields.Nested(
        PropertiesWithSaveSchema,
        missing=dict,
    )


class FilesWithSaveSchema(GeoJsonBaseSchema):
    features = fields.List(
        fields.Nested(FileWithSaveSchema),
        required=True,
    )
