# File: libs/azure/functions/blueprints/aws/athena/activities/monitor.py

from libs.azure.functions import Blueprint
import boto3, os


bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_athena_monitor(ingress: dict):
    session = boto3.Session(
        aws_access_key_id=os.getenv(ingress["access_key"], ingress["access_key"]),
        aws_secret_access_key=os.getenv(ingress["secret_key"], ingress["secret_key"]),
        region_name=os.getenv(ingress.get("region", ""), ingress.get("region", None)),
    )

    athena_client = session.client("athena")

    status = athena_client.get_query_execution(QueryExecutionId=ingress["execution_id"])
    if status["QueryExecution"]["Status"]["State"] in ("FAILED", "CANCELLED"):
        raise Exception(
            "Athena query [{}] failed or was cancelled".format(
                status["QueryExecution"]["Query"]
            )
        )
    elif status["QueryExecution"]["Status"]["State"] in ("QUEUED", "RUNNING"):
        return ""

    return session.client("s3").generate_presigned_url(
        "get_object",
        Params={
            "Bucket": os.environ["DISTILLED_BUCKET"],
            "Key": "/".join(
                status["QueryExecution"]["ResultConfiguration"]["OutputLocation"].split(
                    "/"
                )[3:]
            ),
        },
        ExpiresIn=ingress.get("expires_in", 60 * 60 * 48),
    )
