#File Path: libs/utils/xandr.py
from io import BytesIO
import fastavro
import pandas as pd

def dataframe_to_xandr_avro(
    df: pd.DataFrame, segment_id: int, member_id: int, expiration: int
):
    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "uid": {
                    "device_id": {
                        "id": row["device_id"],
                        "domain": "aaid",
                    }
                },
                "segments": [
                    {
                        "id": segment_id,
                        "code": "",
                        "member_id": member_id,
                        "expiration": expiration,
                        "timestamp": 0,
                        "value": 0,
                    }
                ],
            }
        )
        records.append(
            {
                "uid": {
                    "device_id": {
                        "id": row["device_id"],
                        "domain": "idfa",
                    }
                },
                "segments": [
                    {
                        "id": segment_id,
                        "code": "",
                        "member_id": member_id,
                        "expiration": expiration,
                        "timestamp": 0,
                        "value": 0,
                    }
                ],
            }
        )

    # schema
    xandr_schema = {
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
                            {
                                "name": "id",
                                "type": "string",
                                "doc": "IFA in UUID format.",
                            },
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
                            {
                                "name": "id",
                                "type": "string",
                                "doc": "Mobile device ID.",
                            },
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

    buffer = BytesIO()
    fastavro.writer(buffer, xandr_schema, records)
    buffer.seek(0)
    return buffer