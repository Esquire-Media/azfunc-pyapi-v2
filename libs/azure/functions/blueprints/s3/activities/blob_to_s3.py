# File: libs/azure/functions/blueprints/s3/activities/blob_to_s3.py

from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
import boto3, os

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def blob_to_s3(ingress: dict):
    """
    Transfer data from an Azure Blob to an Amazon S3 object.

    This function fetches data from a specified Azure Blob and uploads it to an Amazon S3 object.
    For larger blobs, the data is uploaded using the S3 multipart upload feature to enhance efficiency.

    Parameters
    ----------
    ingress : dict
        Configuration details for the transfer:
        - source (str/dict): Specifies the Azure Blob to fetch data from.
            If provided as a string, it is assumed to be the Blob URL with a SAS token that has read permissions.
            If provided as a dict, it should contain:
                - conn_str (str): Azure connection string key in environment variables.
                - container_name (str): Azure Blob storage container name.
                - blob_name (str): Azure Blob name.
        - target (dict): Specifies the Amazon S3 object to upload data to. It should contain:
            - access_key (str): AWS access key or environment variable key containing it.
            - secret_key (str): AWS secret access key or environment variable key containing it.
            - bucket (str): S3 bucket name or environment variable key containing it.
            - object_key (str): S3 object key.

    Returns
    -------
    dict
        Response from the S3 upload operation.

    Examples
    --------
    To transfer data from an Azure Blob to an S3 object using an orchestrator function:

    .. code-block:: python

        import azure.durable_functions as df

        def orchestrator_function(context: df.DurableOrchestrationContext):
            response = yield context.call_activity('blob_to_s3', {
                "source": "https://yourazurebloburl",
                "target": {
                    "access_key": "YOUR_AWS_ACCESS_KEY_ENV_VARIABLE",
                    "secret_key": "YOUR_AWS_SECRET_KEY_ENV_VARIABLE",
                    "bucket": "your-s3-bucket",
                    "object_key": "your/s3/object/key"
                }
            })
            return response

    Notes
    -----
    - If the Azure Blob's size exceeds the predefined chunk size (5MB), the data is uploaded to S3 using multipart upload.
    - Ensure that the provided AWS credentials have the necessary permissions for the S3 upload operation.
    - The Azure Blob's size is retrieved to determine the upload method (single or multipart).
    """

    chunk_size = 5 * 1024 * 1024  # Define the size of each chunk for multipart upload

    # Initialize Azure Blob client
    if isinstance(ingress["source"], str):
        blob = BlobClient.from_blob_url(ingress["source"])
    else:
        blob = BlobClient.from_connection_string(
            conn_str=os.environ[ingress["source"]["conn_str"]],
            container_name=ingress["source"]["container_name"],
            blob_name=ingress["source"]["blob_name"],
        )

    # Initialize S3 client using credentials from environment variables
    s3_client = boto3.Session(
        aws_access_key_id=os.getenv(
            ingress["target"]["access_key"], ingress["target"]["access_key"]
        ),
        aws_secret_access_key=os.getenv(
            ingress["target"]["secret_key"], ingress["target"]["secret_key"]
        ),
    ).client("s3")
    s3_bucket = os.getenv(ingress["target"]["bucket"], ingress["target"]["bucket"])
    s3_key = ingress["target"]["object_key"]

    # If the blob's size exceeds the chunk size, perform a multipart upload to S3
    blob_size = blob.get_blob_properties().size
    if blob_size == 0:
        raise Exception("Segment blob is empty.")
    elif blob_size > chunk_size:
        s3_upload_id = s3_client.create_multipart_upload(
            Bucket=s3_bucket,
            Key=s3_key,
        )["UploadId"]
        s3_chunks = []

        # Upload each chunk to S3
        # for index, chunk in enumerate(blob.download_blob().chunks()):
        for index, offset in enumerate(range(0, blob_size, chunk_size)):
            r = s3_client.upload_part(
                Bucket=s3_bucket,
                Key=s3_key,
                PartNumber=index + 1,
                UploadId=s3_upload_id,
                Body=blob.download_blob(offset=offset, length=chunk_size).read(),
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
        
    return ""
