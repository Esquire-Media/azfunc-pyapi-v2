# File: libs/azure/functions/blueprints/aws/athena/activities/execute.py

from azure.durable_functions import Blueprint
import boto3, os


bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def aws_athena_activity_execute(ingress: dict):
    athena_client = boto3.Session(
        aws_access_key_id=os.getenv(ingress["access_key"], ingress["access_key"]),
        aws_secret_access_key=os.getenv(ingress["secret_key"], ingress["secret_key"]),
        region_name=os.getenv(ingress.get("region", ""), ingress.get("region", None)),
    ).client("athena")

    return athena_client.start_query_execution(
        QueryString=ingress["query"],
        QueryExecutionContext={
            'Database': os.getenv(ingress["database"], ingress["database"])
        },
        ResultConfiguration={
            'OutputLocation': "s3://"+os.getenv(ingress["bucket"], ingress["bucket"]),
        },
        WorkGroup=os.getenv(ingress["workgroup"], ingress["workgroup"])
    )['QueryExecutionId']