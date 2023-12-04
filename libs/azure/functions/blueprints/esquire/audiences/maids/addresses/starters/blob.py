# # File: libs/azure/functions/blueprints/esquire/audiences/maids/addresses/starters/blob.py

# from azure.durable_functions import DurableOrchestrationClient
# from azure.functions import InputStream
# from libs.azure.functions import Blueprint
# from libs.data import from_bind

# bp = Blueprint()

# source = {
#     "conn_str": "ONSPOT_CONN_STR",
#     "container_name": "general",
#     "blob_prefix": "audiences",
# }


# @bp.blob_trigger(
#     arg_name="blob",
#     path="{}/{}/{{audienceId}}/addresses.csv".format(
#         source["container_name"],
#         source["blob_prefix"],
#     ),
#     connection="AzureWebJobsStorage",
# )
# @bp.durable_client_input(client_name="client")
# async def starter_esquireAudiencesMaidsAddresses_blob(
#     blob: InputStream, client: DurableOrchestrationClient
# ):
#     provider = from_bind("salesforce")
#     qf = provider["dbo.Audience__c"]
#     audiences = qf[qf["Id"] == blob.name.split("/")[-2]]()
#     if audiences:
#         await client.start_new(
#             orchestration_function_name="orchestrator_esquireAudiencesMaids_fetch",
#             client_input={
#                 "audiences": [
#                     {
#                         "id": audience.Id,
#                         "type": audience.Audience_Type__c,
#                     }
#                     for audience in audiences
#                 ],
#                 "source": source,
#                 "working": {
#                     **source,
#                     "blob_prefix": "raw",
#                 },
#                 "destination": {
#                     **source,
#                     "blob_prefix": "audiences",
#                 },
#             },
#         )
