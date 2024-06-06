# File: /libs/azure/functions/blueprints/esquire/audiences/egress/oneview/orchestrators/generateSegment.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
from azure.storage.blob import BlobClient
import pandas as pd
import logging

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
    path = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPaths",
        **ingress['blobInfo'],
    )
    
    logging.warning(path)
    # 
    # blob = BlobClient.from_connection_string()
    # blob
    
    
    # df = pd.read_csv(ingress["blobUrl"])
    
    # blob.create_append_blob()
    # blob.append_block(df.assign(dt="IDFA",si="asdfasdf").to_csv(header=False, index=False))
    # blob.append_block(df.assign(dt="GOOGLE_AD_ID",si="asdfasdf").to_csv(header=False, index=False))
    # blob.close()

    return {}
