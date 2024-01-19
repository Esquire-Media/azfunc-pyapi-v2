# File: libs/azure/functions/blueprints/esquire/audiences/foursquare_sync/orchestrators/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from libs.azure.functions import Blueprint
from datetime import date, timedelta
import azure.durable_functions as df
import boto3
import os
import pandas as pd
import re

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_foursquaresSync_fetchData(context: DurableOrchestrationContext):
    """
    Orchestrator function for moving the Foursquare data from S3 to Azure.
    
    Parameters
    ----------
    context : DurableOrchestrationContext
        The context for the orchestration, containing methods for interacting with
        the Durable Functions runtime.
    """
    retry = RetryOptions(15000, 1)
    ingress = context.get_input()
    # NOTE ingress = {
    #     "runtime_container": {
    #         "conn_str": conn_str,
    #         "container_name": "foursquare-data",
    #     },
    # }

    if not context.is_replaying:
        # connect to s3 service client
        session = boto3.Session(
            aws_access_key_id=os.environ["fsq_s3_access_key"],
            aws_secret_access_key=os.environ["fsq_s3_secret_key"],
            region_name=os.environ["fsq_s3_region"],
        )
        s3 = session.client("s3")

        # get a list of all s3 objects with prefix
        response = s3.list_objects_v2(
            Bucket=os.environ["fsq_s3_bucket"],
            Prefix=f'{os.environ["fsq_s3_prefix"]}',
        )

        # use the date strings in the filename to find the most recent file
        objs = pd.DataFrame(response["Contents"])
        objs = objs[objs["Size"] > 0]
        objs["Date"] = objs["Key"].apply(
            lambda x: date.fromisoformat(
                re.findall("([0-9]{4}-[0-9]{2}-[0-9]{2})", x)[0]
            )
        )
        recent = objs.sort_values("Date", ascending=False).iloc[0]

        # build an S3 URL for the desired object
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": os.environ["fsq_s3_bucket"], "Key": recent["Key"]},
            ExpiresIn=3600,  # URL expires in 60 minutes
        )

    # async transfer the Foursquare flatfile data from s3 to Azure blob storage
    blob = yield context.call_activity_with_retry(
        "activity_datalake_copyBlob",
        retry,
        {
            "source":url,
            "target":{
                "conn_str":ingress['runtime_container']['conn_str'],
                "container_name":ingress['runtime_container']['container_name'],
                "blob_name":f"{context.instance_id}"
            }
        },
    )

    # wait until async transfer completes
    while True:
        status = yield context.call_activity_with_retry(
            "Activity_AsyncBlobUpload", retry, {**ctx, "blob_path": "raw.tsv.gz"}
        )
        if status == "pending":
            wait_period = context.current_utc_datetime + timedelta(minutes=1)
            yield context.create_timer(wait_period)
        else:
            break

    return blob