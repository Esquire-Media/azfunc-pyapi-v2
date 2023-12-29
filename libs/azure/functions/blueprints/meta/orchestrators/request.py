# File: libs/azure/functions/blueprints/meta/orchestrators/request.py

# from aiopenapi3 import ResponseSchemaError
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint
import json, logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def meta_orchestrator_request(context: DurableOrchestrationContext):
    """
    Orchestrator function to manage Meta API requests.

    This function orchestrates a series of API requests using the activity function
    'meta_activity_request'. It handles pagination, retries, and error management
    across the requests. It aggregates data from all pages of the API response.

    The expected input 'ingress' (obtained from context.get_input()) should be a
    dictionary with specific keys to guide the operation:

    - 'operationId': (required) A unique identifier for the API operation.
    - 'data': (optonal) A dictionary that will be used in the body passed to the API
    - 'parameters': (optional) A dictionary of parameters to be passed to the API.
    - 'recursive': (optional) A boolean indicating whether to recursively fetch all pages.
    - 'return': (optional) A boolean indicating if the response data should be returned.
    - 'destination': (optional) A dictionary specifying Azure Blob Storage details for data storage.
        - 'conn_str' (connection string)
        - 'container_name' 
        - 'blob_prefix'

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context object providing orchestration features like input retrieval,
        activity function calling, and custom status setting.

    Returns
    -------
    list or dict
        The aggregated data from all pages of the API response. Returns a list if multiple
        items are retrieved, or a single dict if only one item is retrieved. Returns an
        empty list if there is no data to return.

    """
    retry = RetryOptions(15000, 5)  # Set retry options for activity calls
    ingress = context.get_input()   # Retrieve input data
    after = None  # Variable to manage pagination
    page = 0      # Page counter
    data = []     # List to aggregate data from all pages
    schema_retry = 0  # Counter for schema-related retries

    while True:
        # Handling retries for schema-related errors
        if schema_retry > 3:
            message = f"Too many retries for Operation {ingress['operationId']}."
            context.set_custom_status(message)
            logging.error(message)
            break

        try:
            # Set status and make a call to the activity function
            message = f"Requesting page {page} for Operation {ingress['operationId']}."
            context.set_custom_status(message)
            if not context.is_replaying:
                logging.warning(message)
            response: dict = yield context.call_activity_with_retry(
                "meta_activity_request",
                retry,
                {
                    **ingress,
                    "parameters": {
                        **ingress["parameters"],
                        **({"after": after} if after else {}),
                    },
                },
            )
        except Exception as e:
            # Log and increment the retry counter on exception
            logging.error(e)
            schema_retry += 1
            continue

        # Process the response from the activity function
        if response:
            if response.get("error"):
                # Handle different error codes
                match response["error"]["code"]:
                    # Throttling errors
                    case 4 | 17 | 80004:
                        # Calculate throttle time and set a timer
                        throttle = (
                            max(
                                [
                                    a["estimated_time_to_regain_access"]
                                    for t in json.loads(
                                        response["headers"]["X-Business-Use-Case-Usage"]
                                    ).values()
                                    for a in t
                                ]
                            )
                            if "X-Business-Use-Case-Usage" in response["headers"].keys()
                            else 0
                        )
                        timer = datetime.utcnow() + timedelta(minutes=throttle)
                        context.set_custom_status(
                            f"Waiting to get page {page}. Throttled until {timer.isoformat()}."
                        )
                        yield context.create_timer(timer)
                        continue
                    # Permissions error
                    case 10:
                        break
                    # Other errors
                    case _:
                        # Raise an exception for other error codes
                        raise Exception(
                            "{} ({}): {}".format(
                                response["error"]["message"],
                                response["error"]["code"],
                                response["error"].get("error_user_msg", ""),
                            )
                        )
            else:
                # Aggregate data from the response
                if response.get("data"):
                    if isinstance(response["data"], list):
                        data += response["data"]
                    else:
                        data.append(response["data"])
                else:
                    data.append(response)

                # Handle pagination
                if ingress.get("recursive") and response["after"]:
                    after = response["after"]
                    page += 1
                    continue
        break
    context.set_custom_status(f"All requests completed.")

    # Return the aggregated data or an empty list
    if ingress.get("return", True) or ingress.get("destination", {}):
        if len(data) == 1:
            return data[0]
        return data
    return []
