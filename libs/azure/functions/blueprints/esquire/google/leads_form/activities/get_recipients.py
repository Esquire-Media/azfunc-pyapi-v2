from azure.durable_functions import Blueprint
from azure.data.tables import TableClient
import os

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()


# Define an activity function
@bp.activity_trigger(input_name="ingress")
def activity_googleLeadsForm_getRecipients(ingress: dict):

    # if a campaign proposal conn string is set, use that. Otherwise use AzureWebJobsStorage
    conn_str = (
        "GOOGLE_LEADS_CONN_STR"
        if "GOOGLE_LEADS_CONN_STR" in os.environ.keys()
        else "AzureWebJobsStorage"
    )

    # load email recipients based on form_id
    table_client = TableClient.from_connection_string(
        conn_str=os.environ[conn_str], table_name="formRoutes"
    )
    entities = table_client.query_entities(f"PartitionKey eq '{ingress['form_id']}'")

    return [entity["Recipient"] for entity in entities]
