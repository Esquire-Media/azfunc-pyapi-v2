from azure.storage.blob import BlobClient, ContainerClient
from libs.azure.functions import Blueprint
from libs.utils.azure_storage import get_blob_sas
import pandas as pd, os, uuid, fsspec

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceOneview_collateAudience(ingress: dict) -> dict:
    destination_container = ContainerClient.from_connection_string(
        conn_str=os.environ[ingress["destination"]["conn_str"]],
        container_name=ingress["destination"]["container_name"],
    )
    if not destination_container.exists():
        destination_container.create_container()
    destination_blob = destination_container.get_blob_client(
        ingress["destination"].get(
            "blob_name",
            "{}/{}.csv".format(ingress["destination"]["blob_prefix"], uuid.uuid4().hex),
        )
    )

    fs = fsspec.filesystem(
        "az", connection_string=os.environ[ingress["destination"]["conn_str"]]
    )
    with fs.open(
        "{}/{}".format(destination_blob.container_name, destination_blob.blob_name), "w"
    ) as out:
        for blob_name in ingress["sources"]["blob_names"]:
            df = pd.read_csv(
                BlobClient.from_connection_string(
                    conn_str=os.environ[ingress["sources"]["conn_str"]],
                    container_name=ingress["sources"]["container_name"],
                    blob_name=blob_name,
                ).download_blob()
            )
            for device_type in ["IDFA", "GOOGLE_AD_ID"]:
                out.write(
                    df.assign(
                        devicetype=device_type, segmentid=ingress["audience"]["segment"]
                    ).to_csv(header=False, index=False, mode="a")
                )

    return get_blob_sas(destination_blob)
