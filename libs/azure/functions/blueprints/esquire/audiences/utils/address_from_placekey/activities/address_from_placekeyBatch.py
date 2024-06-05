# File: libs/azure/functions/blueprints/esquire/audiences/ingress/mover_sync/activities/validate_address_chunks.py

from libs.azure.functions import Blueprint
from azure.data.tables import TableClient
from datetime import timedelta
import os

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

freshness_window = timedelta(days=365)


# Define an activity function
@bp.activity_trigger(input_name="ingress")
def activity_addresses_fromPlacekeyBatch(ingress: list):
    """
    ingress:
    [
        "228@647-5c5-7wk",
        ...
    ]

    return:
    [
            {
                "street":"123 Main St",
                "city":"Chicago",
                "state":"IL",
                "zipcode":"12345"
            },
            ...
        ]
    """

    # connect to placekey cache table
    table = TableClient.from_connection_string(
        conn_str=os.environ["ADDRESSES_CONN_STR"], table_name="placekeys"
    )

    # query and format the address entities
    queried_entities = []
    for placekey in ingress:
        entities = table.query_entities(f"PartitionKey eq '{placekey}'")
        [
            queried_entities.append(
                {
                    "placekey": entity["PartitionKey"],
                    **{
                        k: v
                        for k, v in entity.items()
                        if k in ["street", "city", "state", "zipcode"]
                    },
                }
            )
            for entity in entities
        ]

    return queried_entities
