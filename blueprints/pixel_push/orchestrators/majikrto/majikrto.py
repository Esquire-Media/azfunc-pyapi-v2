# from azure.durable_functions import DurableOrchestrationContext, RetryOptions
# from libs.azure.functions import Blueprint
# import logging
# from libs.utils.azure_storage import load_dataframe

# bp = Blueprint()

# # TODO - Once deployed, will need to add smarty keyvault access


# @bp.orchestration_trigger(context_name="context")
# def orchestrator_pixelPush_majikrtoFormatting(context: DurableOrchestrationContext):
    
#     # Use latlong to calculate nearest store (from stores2.csv)
#     yield context.call_activity_with_retry(
#         "activity_pixelPush_calculateStoreDistances",
#         retry,
#         {
#             "source": users_validated_url,
#             "store_locations_source": settings["store_locations_source"],
#             "destination": {
#                 **settings["runtime_container"],
#                 "blob_name": f"{settings['account']}/{context.instance_id}/users/02_distances",
#             },
#         },
#     )