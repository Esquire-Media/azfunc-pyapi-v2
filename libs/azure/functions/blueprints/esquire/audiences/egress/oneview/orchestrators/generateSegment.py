# File: /libs/azure/functions/blueprints/esquire/audiences/egress/oneview/orchestrators/generateSegment.py

from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os, pandas as pd

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudienceOneView_generateSegment(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    # ingress = {
    #     "blobInfo": {
    #         "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
    #         "container_name": os.environ["ESQUIRE_AUDIENCE_CONTAINER_NAME"],
    #         "audience_id": audience_id,
    #     },
    #     "audience_id": audience_id,
    #     "segmentId": ids['segment'],
    # }

    # Fetch the mpst recent path to the audience files
    blobPaths = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPaths",
        **ingress["blobInfo"],
    )

    # create the appendblob
    appendBlob = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["blobInfo"]["conn_str"]],
        container_name=ingress["blobInfo"]["container_name"],
        blob_name=f"{'/'.join(blobPaths[0].split('/')[:-1]) + '/'}appendBlob/blob.csv",
    )#
    
    appendBlob.create_append_blob()
    # for each blob, read the csv, append them to the new csv
    for blobPath in blobPaths:
        blob = BlobClient.from_connection_string(
            conn_str=os.environ[ingress["blobInfo"]["conn_str"]],
            container_name=ingress["blobInfo"]["container_name"],
            blob_name=blobPath,
        )
        df = pd.read_csv(
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
        appendBlob.append_block(
            df.assign(dt="IDFA", si="asdfasdf").to_csv(header=False, index=False)
        )
        appendBlob.append_block(
            df.assign(dt="GOOGLE_AD_ID", si="asdfasdf").to_csv(
                header=False, index=False
            )
        )

    # close connection to appendBlob
    appendBlob.close()

    return appendBlob.blob_name
