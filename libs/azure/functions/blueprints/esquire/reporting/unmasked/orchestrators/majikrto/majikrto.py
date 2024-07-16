from azure.durable_functions import Blueprint, DurableOrchestrationContext, RetryOptions

bp = Blueprint()

# TODO - Once deployed, will need to add smarty keyvault access


@bp.orchestration_trigger(context_name="context")
def orchestrator_pixelPush_majikrtoFormatting(context: DurableOrchestrationContext):

    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # call smarty activity for address validation
    validated_blob_url = yield context.call_activity_with_retry(
        "activity_smarty_validateAddresses",
        retry,
        {
            "source": ingress["source"],
            "column_mapping": {
                "street": "personal_address",
                "city": "personal_city",
                "state": "personal_state",
                "zip": "personal_zip",
            },
            "destination": {
                **ingress["runtime_container"],
                "blob_name": f"{ingress['parent_instance']}/{ingress['client']}/{ingress['data_pull_name']}/01_validated.csv",
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
    distances_url = yield context.call_activity_with_retry(
        "activity_pixelPush_calculateStoreDistances",
        retry,
        {
            "source": validated_blob_url,
            "store_locations_source": {
                "conn_str": "AzureWebJobsStorage",
                "container_name": "pixel-push",
                "blob_name": f"majikrto/store_locations.csv",
            },
            "destination": {
                **ingress["runtime_container"],
                "blob_name": f"{ingress['parent_instance']}/{ingress['client']}/{ingress['data_pull_name']}/02_distances.csv",
            },
        },
    )

    return distances_url
