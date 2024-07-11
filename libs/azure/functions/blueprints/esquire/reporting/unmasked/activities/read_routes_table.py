from azure.durable_functions import Blueprint
from azure.data.tables import TableClient
import pandas as pd
import os
import logging

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="ingress")
def activity_pixelPush_readRoutesTable(ingress: dict):
    """
    Returns a list of pixel routes with the following format:

    [
        {
            'PartitionKey': 'majikrto',
            'RowKey': 'events',
            'formatting_orchestrator': ' ',
            'url': 'https://esquire-callback-reader.azurewebsites.net/api/esquire/callback_reader'
        },
        {
            'PartitionKey': 'majikrto',
            'RowKey': 'users',
            'formatting_orchestrator': ' orchestrator_pixelPush_majikrtoFormatting'
            'url': 'https://esquire-callback-reader.azurewebsites.net/api/esquire/callback_reader',
        }
    ]
    """
    
    # connect to pixel routes table
    pixel_routes = TableClient.from_connection_string(
        conn_str=os.environ['AzureWebJobsStorage'],
        table_name="pixelRoutes"
    )
    entities = pixel_routes.query_entities("enabled eq true")
    result = [*entities]

    return result