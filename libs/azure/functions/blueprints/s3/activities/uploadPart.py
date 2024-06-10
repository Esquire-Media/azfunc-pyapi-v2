from libs.azure.functions import Blueprint
import boto3
import requests

from libs.utils.azure_storage import get_blob_sas, init_blob_client

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_s3_uploadPart(ingress: dict) -> dict:
    s3_client = boto3.client('s3')
    destination_bucket = ingress["destination"]["bucket"]
    destination_key = ingress["destination"]["key"]

    try:
        source_blob = init_blob_client(**ingress["source"])
        source_url = get_blob_sas(source_blob)
        source_size = source_blob.get_blob_properties().size
    except:
        source_url = ingress["source"]["url"]
        source_size = int(requests.head(source_url).headers["Content-Length"])

    max_block_size = 4 * 1024 * 1024  # Adjust the block size as needed

    upload_id = ingress.get("upload_id")
    if not upload_id:
        response = s3_client.create_multipart_upload(Bucket=destination_bucket, Key=destination_key)
        upload_id = response['UploadId']

    part_infos = []
    for i in range(0, source_size, max_block_size):
        part_number = (i // max_block_size) + 1
        part = s3_client.upload_part_copy(
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            CopySourceRange=f'bytes={i}-{min(i + max_block_size, source_size) - 1}',
            PartNumber=part_number,
            UploadId=upload_id
        )
        part_infos.append({
            'ETag': part['CopyPartResult']['ETag'],
            'PartNumber': part_number
        })

    return {
        "index": ingress.get("index", 0),
        "ETag": part_infos[-1]['ETag'],
        "upload_id": upload_id
    }
