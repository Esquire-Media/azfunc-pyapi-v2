# File: libs/azure/functions/blueprints/s3/activities/list_objects.py

from azure.durable_functions import Blueprint
import boto3, os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def s3_list_objects(ingress: dict) -> list:
    """
    List object keys from an S3 bucket based on specified parameters.

    This function fetches object keys from an S3 bucket, filters out empty objects,
    sorts the results in descending order based on the key name, and returns the first 11 keys.

    Parameters
    ----------
    ingress : dict
        A dictionary containing necessary parameters, specified as follows:
        - access_key (str): AWS Access Key ID. Can also be an environment variable name that stores the Access Key.
        - secret_key (str): AWS Secret Access Key. Can also be an environment variable name that stores the Secret Key.
        - region (str): AWS region name. Can also be an environment variable name that stores the region.
        - bucket (str): Name of the S3 bucket.
        - prefix (str, optional): The prefix (folder path) to filter the object keys.

    Returns
    -------
    list
        A list containing up to 11 sorted object keys from the S3 bucket, in descending order.

    Examples
    --------
    In an orchestrator function:

    .. code-block:: python

        import azure.durable_functions as df

        def orchestrator_function(context: df.DurableOrchestrationContext):
            s3_keys = yield context.call_activity('s3_list_objects', {
                "access_key": "MY_ACCESS_KEY",
                "secret_key": "MY_SECRET_KEY",
                "region": "us-west-1",
                "bucket": "my-bucket",
                "prefix": "data/"
            })
            return s3_keys

    Notes
    -----
    - The function utilizes the `list_objects_v2` operation of S3 with pagination.
    - It's essential to ensure that the AWS credentials provided have the necessary permissions to list objects in the specified bucket.
    """
    # Initialize S3 client using credentials from environment variables
    s3_client = boto3.Session(
        aws_access_key_id=os.getenv(ingress["access_key"], ingress["access_key"]),
        aws_secret_access_key=os.getenv(ingress["secret_key"], ingress["secret_key"]),
        region_name=os.getenv(ingress["region"], ingress["region"]),
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
                    Bucket=ingress["bucket"],
                    Prefix=ingress["prefix"],
                )
                if "Contents" in page.keys()
                for obj in page["Contents"]
                if obj["Size"] > 0
            ],
            key=lambda d: d["Key"],
            reverse=True,
        )
    ]
