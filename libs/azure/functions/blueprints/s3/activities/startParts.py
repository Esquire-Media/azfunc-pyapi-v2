from libs.azure.functions import Blueprint
import boto3

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_s3_startPart(ingress: dict) -> dict:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=ingress["access_key"],
        aws_secret_access_key=ingress["secret_key"],
    )
    try:
        return s3_client.create_multipart_upload(
            Bucket=ingress["bucket"], Key=ingress["object_key"]
        )["UploadId"]
    except Exception as e:
        raise Exception(f"Failed to initiate multipart upload: {str(e)}")
