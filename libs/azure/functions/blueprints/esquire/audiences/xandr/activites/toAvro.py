# File: /libs/azure/functions/blueprints/esquire/audiences/xandr/activities/toAvro.py

from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
import fastavro
import logging

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
                        {"name": "id", "type": "string", "doc": "External ID provided by member."},
                        {"name": "member_id", "type": "int", "doc": "Owner member ID.", "default": 0},
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
                        {"name": "id", "type": "int", "doc": "Segment ID. Alternatively, pair of code and member_id can be used.", "default": 0},
                        {"name": "code", "type": "string", "doc": "Segment code. Requires segment.member_id.", "default": ""},
                        {"name": "member_id", "type": "int", "doc": "Segment member ID. Requires segment.code.", "default": 0},
                        {"name": "expiration", "type": "int", "doc": "Segment expiration in minutes. 0: max expiration (180 days); -2: default expiration; -1: segment removal.", "default": 0},
                        {"name": "timestamp", "type": "long", "doc": "Defines when segment becomes 'live'. Timestamp in seconds from epoch. 0 enables segment immediately", "default": 0},
                        {"name": "value", "type": "int", "doc": "User provided value associated with the segment.", "default": 0},
                    ],
                },
            },
        },
    ],
}

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceXandr_generateAvro(ingress: dict):
    # ingress = {
    #     "xandr_segment_id": "",
    #     "maids_url":"",
    #     "expiration": "",
    #     "temp_file":"",
    # }

    # get a list of all the device_ids
    maids_full = []
    for key, value in sorted(ingress["maids_url"].items()):
        if key.startswith("Blob_"):
            maids = (
                BlobClient.from_blob_url(value["url"])
                .download_blob()
                .readall()
                .decode("utf-8")
                .split("\r\n")[1:-1]
            )
            maids_full.extend(maids)

    # test the full maids list
    logging.warning(len(maids_full))
    # Establish base format dict
    json_dict = {
        "uid": {"device_id": {"id": "device_id", "domain": "device_type"}},
        "segments": [
            {
                "id": ingress["xandr_segment_id"],
                "code": "",
                "member_id": 12345,  # static for now
                "expiration": 1440,  # static for now
                "timestamp": 0,
                "value": 0,
            }
        ],
    }

    # Create a list to hold all records
    records = []

    # Iterate through the list of device IDs and append to final records list
    for device_id in maids_full:
        # code for first device type
        device_type = "aaid"  # Replace with actual device type logic if needed
        # Update json_dict
        json_dict["uid"]["device_id"]["id"] = device_id
        json_dict["uid"]["device_id"]["domain"] = device_type
        # Add json_dict to records list
        records.append(json_dict.copy())
        # code for second device type
        device_type = "idfa"  # Replace with actual device type logic if needed
        # Update json_dict
        json_dict["uid"]["device_id"]["id"] = device_id
        json_dict["uid"]["device_id"]["domain"] = device_type
        # Add json_dict to records list
        records.append(json_dict.copy())

    # Write records to avro file
    output_file = f'{ingress["xandr_segment_id"]}_device_ids_testing.avro'
    with open(output_file, "wb") as out:
        fastavro.writer(out, SCHEMA, records)

    logging.warning(f"Completed Avro File of {len(maids_full)} Devices")

    return output_file
