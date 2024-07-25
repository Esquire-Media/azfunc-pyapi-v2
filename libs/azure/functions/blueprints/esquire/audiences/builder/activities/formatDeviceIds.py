# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/DeviceIds.py

from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas,
)
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import StringIO
from urllib.parse import unquote
import os, uuid

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_formatDeviceIds(ingress: dict):
    # Initialize the destination BlobClient
    output_blob = BlobClient.from_connection_string(
        conn_str=os.environ[ingress["destination"]["conn_str"]],
        container_name=ingress["destination"]["container_name"],
        blob_name=ingress["destination"].get(
            "blob_name",
            "{}/{}.csv".format(ingress["destination"]["blob_prefix"], uuid.uuid4().hex),
        ),
    )

    # Create new blob with header prefixed
    output_blob.stage_block(
        block_id=(block_header := str(uuid.uuid4())), data=StringIO("deviceid\n")
    )
    try:
        output_blob.stage_block_from_url(
            block_id=(block_data := str(uuid.uuid4())), source_url=ingress["source"]
        )
        output_blob.commit_block_list([block_header, block_data])
    except:
        output_blob.commit_block_list([block_header])

    # Generate a SAS token for the destination blob with read permissions
    sas_token = generate_blob_sas(
        account_name=output_blob.account_name,
        container_name=output_blob.container_name,
        blob_name=output_blob.blob_name,
        account_key=output_blob.credential.account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )

    # Return the URL of the destination blob with the SAS token
    return unquote(output_blob.url) + "?" + sas_token
