from azure.durable_functions import Blueprint
from azure.data.tables import TableClient
from azure.functions import TimerRequest
from azure.storage.blob import ContainerClient
import os

bp = Blueprint()


@bp.timer_trigger(arg_name="timer", schedule="0 0 */2 * * *")
def timer_locationInsights_UpdateAssetsTable(timer: TimerRequest):
    """
    Updates the Storage Table "locationInsightsAssets" with the names of each asset package currently in Blob Storage.
    This table will be the quick-access way to query the valid templates, creative_sets, and any other dynamic asset packages.
    Runs every 2 hours.
    """

    # if a campaign proposal conn string is set, use that. Otherwise use AzureWebJobsStorage
    conn_str = (
        "LOCATION_INSIGHTS_CONN_STR"
        if "LOCATION_INSIGHTS_CONN_STR" in os.environ.keys()
        else "AzureWebJobsStorage"
    )
    resources_container = {  # container for prebuilt assets
        "conn_str": conn_str,
        "container_name": "location-insights-resources",
    }
    assets_table = {  # table of valid asset package names
        "conn_str": conn_str,
        "table_name": "locationInsightsAssets",
    }

    # connect to resource storage client
    container_client = ContainerClient.from_connection_string(
        conn_str=os.environ[resources_container["conn_str"]],
        container_name=resources_container["container_name"],
    )
    # connect to assets table client
    table_client = TableClient.from_connection_string(
        conn_str=os.environ[assets_table["conn_str"]],
        table_name=assets_table["table_name"],
    )

    # get unique resource packages in each of the pptx-resource directories
    templates = list(
        set(
            [
                name.split("/")[1].split(".")[0]
                for name in container_client.list_blob_names(
                    name_starts_with="templates/"
                )
            ]
        )
    )
    creative_sets = list(
        set(
            [
                name.split("/")[1]
                for name in container_client.list_blob_names(
                    name_starts_with="creatives/"
                )
            ]
        )
    )
    promotion_sets = list(
        set(
            [
                name.split("/")[1]
                for name in container_client.list_blob_names(
                    name_starts_with="promotions/"
                )
            ]
        )
    )

    # add template names
    for template in templates:
        table_client.upsert_entity(
            entity={"PartitionKey": "template", "RowKey": template}
        )
    # add creative set names
    for creative_set in creative_sets:
        table_client.upsert_entity(
            entity={"PartitionKey": "creativeSet", "RowKey": creative_set}
        )
    # add promotion set names
    for promotion_set in promotion_sets:
        table_client.upsert_entity(
            entity={"PartitionKey": "promotionSet", "RowKey": promotion_set}
        )

    # remove assets that no longer exist
    for row in table_client.list_entities():
        if row["RowKey"] not in templates + creative_sets + promotion_sets:
            table_client.delete_entity(row)
