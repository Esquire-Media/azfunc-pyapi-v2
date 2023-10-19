# File: libs/azure/functions/blueprints/oneview/segments/activities/upload_segment_file.py

from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from libs.azure.functions import Blueprint
import boto3, os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def oneview_segments_upload_segment_file(ingress: dict):
    """
    Upload segment data from Azure Blob Storage to AWS S3.

    This function retrieves a segment data blob from Azure Blob Storage and uploads it
    to a specified S3 bucket. Depending on the blob's size, it either performs a single
    upload or a multipart upload.

    Parameters
    ----------
    ingress : dict
        Dictionary containing details about the Azure Blob Storage, the blob's name,
        and the record's SegmentID.

    Returns
    -------
    str
        A SAS URL to the blob in Azure Blob Storage.

    Notes
    -----
    This function uses the azure.storage.blob library to interact with Azure Blob Storage
    and the boto3 library to interact with AWS S3.
    """

    chunk_size = 5 * 1024 * 1024  # Define the size of each chunk for multipart upload

    # Initialize Azure Blob client using connection string from environment variables
    blob: BlobClient = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["output"]["conn_str"]],
        container_name=ingress["output"]["container_name"],
        blob_name=ingress["blob_name"],
        max_chunk_get_size=chunk_size,
    )

    # Initialize S3 client using credentials from environment variables
    s3_client = boto3.Session(
        aws_access_key_id=os.environ["ONEVIEW_SEGMENTS_AWS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["ONEVIEW_SEGMENTS_AWS_SECRET_KEY"],
    ).client("s3")
    s3_bucket = os.environ["ONEVIEW_SEGMENTS_S3_BUCKET"]
    s3_key = "{}/{}.csv".format(
        os.environ["ONEVIEW_SEGMENTS_S3_PREFIX"],
        ingress["record"]["SegmentID"],
    )

    # If the blob's size exceeds the chunk size, perform a multipart upload to S3
    if blob.get_blob_properties().size > chunk_size:
        s3_upload_id = s3_client.create_multipart_upload(
            Bucket=s3_bucket,
            Key=s3_key,
        )["UploadId"]
        s3_chunks = []

        # Upload each chunk to S3
        for index, chunk in enumerate(blob.download_blob().chunks()):
            r = s3_client.upload_part(
                Bucket=s3_bucket,
                Key=s3_key,
                PartNumber=index + 1,
                UploadId=s3_upload_id,
                Body=chunk,
            )
            s3_chunks.append({"PartNumber": index + 1, "ETag": r["ETag"]})

        # Complete the multipart upload on S3
        s3_client.complete_multipart_upload(
            Bucket=s3_bucket,
            Key=s3_key,
            UploadId=s3_upload_id,
            MultipartUpload={"Parts": s3_chunks},
        )
    # If the blob's size is within the chunk size, perform a single upload to S3
    else:
        s3_client.upload_fileobj(
            Fileobj=blob.download_blob(),
            Bucket=s3_bucket,
            Key=s3_key,
        )

    # Generate a SAS token for the blob in Azure Blob Storage and return the URL
    return (
        blob.url
        + "?"
        + generate_blob_sas(
            account_name=blob.account_name,
            container_name=blob.container_name,
            blob_name=blob.blob_name,
            account_key=blob.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    )
