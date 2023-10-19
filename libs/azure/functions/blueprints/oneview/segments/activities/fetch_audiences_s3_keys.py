# File: libs/azure/functions/blueprints/oneview/segments/activities/fetch_audience_s3_keys.py

from libs.azure.functions import Blueprint
import boto3, os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def oneview_segments_fetch_audiences_s3_keys(ingress: dict):
    """
    Fetch S3 keys for audience data.

    This function retrieves a list of S3 object keys from a specified bucket
    and prefix. The keys represent audience data files in the bucket.
    Only non-empty objects are considered, and the list is sorted in descending
    order based on the object key names. The function returns the first 11 keys.

    Parameters
    ----------
    ingress : dict
        Dictionary containing details about the S3 bucket and prefix.
        Expected to have a 'record' key with nested 'Bucket' and 'Folder' keys.

    Returns
    -------
    list
        A list of S3 object keys.

    Notes
    -----
    This function uses the boto3 library to interact with AWS S3.
    """

    # Initialize S3 client using credentials from environment variables
    s3_client = boto3.Session(
        aws_access_key_id=os.environ["REPORTS_AWS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["REPOSTS_AWS_SECRET_KEY"],
        region_name=os.environ["REPORTS_AWS_REGION"],
    ).client("s3")

    # Get a paginator for the list_objects_v2 S3 operation
    paginator = s3_client.get_paginator("list_objects_v2")

    # Fetch and sort the object keys
    # Iterate through paginated results and filter out empty objects
    # Sort the results in descending order based on key name and return the first 11 keys
    return [
        obj["Key"]
        for obj in sorted(
            [
                obj
                for page in paginator.paginate(
                    Bucket=ingress["record"]["Bucket"],
                    Prefix=ingress["record"]["Folder"],
                )
                if "Contents" in page.keys()
                for obj in page["Contents"]
                if obj["Size"] > 0
            ],
            key=lambda d: d["Key"],
            reverse=True,
        )[0:11]
    ]
