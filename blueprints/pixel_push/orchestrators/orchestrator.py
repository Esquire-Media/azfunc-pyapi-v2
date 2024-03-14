from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from libs.azure.functions import Blueprint
import logging
from libs.utils.azure_storage import load_dataframe

bp = Blueprint()

# TODO - Once deployed, will need to add smarty keyvault access


@bp.orchestration_trigger(context_name="context")
def orchestrator_pixelPush_root(context: DurableOrchestrationContext):
    """
    Orchestrator function for the Unmasked Pixel Push process.

    Coordinates tasks for loading Unmasked pixel insights data for a given client,
    and sending those results to a desired location.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context for the orchestration, containing methods for interacting with
        the Durable Functions runtime.
    """

    settings = context.get_input()
    retry = RetryOptions(15000, 1)

    # BUILD THE USERS BLOB
    # Execute queries for different audience types and store results
    users_url = yield context.call_sub_orchestrator(
        "aws_athena_orchestrator",
        {
            **{k: v for k, v in settings.items() if "query" not in k},
            "query": settings["users_query"],
            "destination": {
                **settings["runtime_container"],
                "blob_name": f"{settings['account']}/{context.instance_id}/users/00_raw",
            },
        },
    )
    # call smarty activity for address validation
    users_validated_url = yield context.call_activity_with_retry(
        "activity_smarty_validateAddresses",
        retry,
        {
            "source": users_url,
            "column_mapping": {
                "street": "personal_address",
                "city": "personal_city",
                "state": "personal_state",
                "zip": "personal_zip",
            },
            "destination": {
                **settings["runtime_container"],
                "blob_name": f"{settings['account']}/{context.instance_id}/users/01_validated",
            },
            "columns_to_return": [
                "hem",
                "first_name",
                "last_name",
                "personal_email",
                "mobile_phone",
                "personal_phone",
                "delivery_line_1",
                "city_name",
                "state_abbreviation",
                "zipcode",
                "latitude",
                "longitude",
            ],
        },
    )
    # Use latlong to calculate nearest store (from stores2.csv)
    yield context.call_activity_with_retry(
        "activity_pixelPush_calculateStoreDistances",
        retry,
        {
            "source": users_validated_url,
            "store_locations_source": settings["store_locations_source"],
            "destination": {
                **settings["runtime_container"],
                "blob_name": f"{settings['account']}/{context.instance_id}/users/02_distances",
            },
        },
    )

    # BUILD THE EVENTS BLOB
    # Execute queries for different audience types and store results
    events_url = yield context.call_sub_orchestrator(
        "aws_athena_orchestrator",
        {
            **{k: v for k, v in settings.items() if "query" not in k},
            "query": settings["events_query"],
            "destination": {
                **settings["runtime_container"],
                "blob_name": f"{settings['account']}/{context.instance_id}/events/00_raw",
            },
        },
    )

    # PUSH DATA TO WEBHOOK
    # Push the Users data
    yield context.call_activity(
        "activity_httpx",
        {
            "method":"POST",
            "url":"https://esquire-callback-reader.azurewebsites.net/api/esquire/callback_reader",
            "data":load_dataframe(users_validated_url).to_csv(index=False),
            "headers":{
                "data":"users",
                "Content-Type":"text/csv"
            }
        }
    )

    # Push the Events data
    yield context.call_activity(
        "activity_httpx",
        {
            "method":"POST",
            "url":"https://esquire-callback-reader.azurewebsites.net/api/esquire/callback_reader",
            "data":load_dataframe(events_url).to_csv(index=False),
            "headers":{
                "data":"events",
                "Content-Type":"text/csv"
            }
        }
    )

    # Call sub-orchestrator to purge the instance history
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )
