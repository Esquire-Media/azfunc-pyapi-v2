# File: /libs/azure/functions/blueprints/esquire/audiences/egress/xandr/activities/generateAvro.py

from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
import fastavro, os, pandas as pd, uuid, fsspec, json

from libs.utils.azure_storage import get_blob_sas

bp = Blueprint()

# Define the schema as a global variable
SCHEMA = {
    "namespace": "xandr.avro",
    "name": "user",
    "type": "record",
    "fields": [
        {
            "name": "uid",
            "doc": "User ID. Can be one of anid, ifa, xfa, external_id, device_id type.",
            "type": [
                {"name": "anid", "type": "long", "doc": "Xandr user ID."},
                {
                    "name": "ifa",
                    "type": "record",
                    "doc": "Identifier for Advertising record by iabtechlab.com",
                    "fields": [
                        {"name": "id", "type": "string", "doc": "IFA in UUID format."},
                        {"name": "type", "type": "string", "doc": "IFA type."},
                    ],
                },
                {
                    "name": "xfa",
                    "type": "record",
                    "doc": "Xandr synthetic ID record.",
                    "fields": [
                        {
                            "name": "device_model_id",
                            "type": "int",
                            "doc": "Device atlas device model.",
                            "default": 0,
                        },
                        {
                            "name": "device_make_id",
                            "type": "int",
                            "doc": "Device atlas device make.",
                            "default": 0,
                        },
                        {
                            "name": "ip",
                            "type": "string",
                            "default": "",
                            "doc": "Residential IP address.",
                        },
                    ],
                },
                {
                    "name": "external_id",
                    "type": "record",
                    "doc": "External ID record.",
                    "fields": [
                        {
                            "name": "id",
                            "type": "string",
                            "doc": "External ID provided by member.",
                        },
                        {
                            "name": "member_id",
                            "type": "int",
                            "doc": "Owner member ID.",
                            "default": 0,
                        },
                    ],
                },
                {
                    "name": "device_id",
                    "type": "record",
                    "doc": "Mobile device ID record.",
                    "fields": [
                        {"name": "id", "type": "string", "doc": "Mobile device ID."},
                        {
                            "name": "domain",
                            "type": {
                                "name": "domain",
                                "type": "enum",
                                "doc": "Mobile device domain.",
                                "symbols": [
                                    "idfa",
                                    "sha1udid",
                                    "md5udid",
                                    "openudid",
                                    "aaid",
                                    "windowsadid",
                                    "rida",
                                ],
                            },
                        },
                    ],
                },
            ],
        },
        {
            "name": "segments",
            "doc": "Array of segments.",
            "type": {
                "type": "array",
                "doc": "Element of the segments array.",
                "items": {
                    "name": "segment",
                    "type": "record",
                    "fields": [
                        {
                            "name": "id",
                            "type": "int",
                            "doc": "Segment ID. Alternatively, pair of code and member_id can be used.",
                            "default": 0,
                        },
                        {
                            "name": "code",
                            "type": "string",
                            "doc": "Segment code. Requires segment.member_id.",
                            "default": "",
                        },
                        {
                            "name": "member_id",
                            "type": "int",
                            "doc": "Segment member ID. Requires segment.code.",
                            "default": 0,
                        },
                        {
                            "name": "expiration",
                            "type": "int",
                            "doc": "Segment expiration in minutes. 0: max expiration (180 days); -2: default expiration; -1: segment removal.",
                            "default": 0,
                        },
                        {
                            "name": "timestamp",
                            "type": "long",
                            "doc": "Defines when segment becomes 'live'. Timestamp in seconds from epoch. 0 enables segment immediately",
                            "default": 0,
                        },
                        {
                            "name": "value",
                            "type": "int",
                            "doc": "User provided value associated with the segment.",
                            "default": 0,
                        },
                    ],
                },
            },
        },
    ],
}


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceXandr_generateAvro(ingress: dict):
    # ingress = {
    #     "audience" : {
    #         "segment": "asdfasdf"
    #         "expiration": 1440
    #     }
    #     "source": {
    #         "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
    #         "container_name": os.environ["ESQUIRE_AUDIENCE_CONTAINER_NAME"],
    #         "blob_name": blob_name,
    #     },
    #     "destination": {
    #         "conn_str": "AzureWebJobsStorage",
    #         "container_name": os.environ["TASK_HUB_NAME"] + "-largemessages",
    #         "blob_prefix": f"{context.instance_id}/",
    #     },
    # }

    if isinstance(ingress["source"], str):
        source_blob = BlobClient.from_blob_url(ingress["source"])
    else:
        source_blob = BlobClient.from_connection_string(
            conn_str=os.environ[ingress["source"]["conn_str"]],
            container_name=ingress["source"]["container_name"],
            blob_name=ingress["source"]["blob_name"],
        )
    df = pd.read_csv(source_blob.download_blob())

    # fs = fsspec
    # if isinstance(ingress["destination"], str):
    #     destination_blob = BlobClient.from_blob_url(ingress["destination"])
    #     destination_path = ingress["destination"]
    # else:
    #     destination_blob = BlobClient.from_connection_string(
    #         conn_str=os.environ[ingress["destination"]["conn_str"]],
    #         container_name=ingress["destination"]["container_name"],
    #         blob_name="{}/{}.avro".format(
    #             ingress["destination"]["blob_prefix"], uuid.uuid4().hex
    #         ),
    #     )
    #     fs = fsspec.filesystem(
    #         "az", connection_string=os.environ[ingress["destination"]["conn_str"]]
    #     )
    #     destination_path = "az://{}/{}".format(
    #         destination_blob.container_name,
    #         destination_blob.blob_name,
    #     )
    fs = fsspec.filesystem(
        "s3",
        key=ingress["destination"]["access_key"],
        secret=ingress["destination"]["secret_key"],
    )

    # with fs.open(destination_path, "wb") as blob:
    with fs.open(
        "s3://{}/submitted/{}.avro".format(
            ingress["destination"]["bucket"], uuid.uuid4().hex
        ),
        "wb",
    ) as out:
        fastavro.writer(
            out,
            SCHEMA,
            [
                {
                    "uid": {
                        "id": device_id,
                        "domain": device_type,
                    },
                    "segments": [
                        {
                            "id": int(ingress["audience"]["segment"]),
                            "code": "",
                            "member_id": int(os.environ["XANDR_MEMBER_ID"]),
                            "expiration": ingress["audience"]["expiration"],
                            "timestamp": 0,
                            "value": 0,
                        }
                    ],
                }
                for device_id in df["deviceid"].to_list()
                for device_type in ["aaid", "idfa"]
            ],
        )

    return ""
