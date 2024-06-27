# from azure.durable_functions import Blueprint
# from azure.functions import HttpRequest, HttpResponse
# from azure.durable_functions import DurableOrchestrationClient
# import logging
# from pydantic import BaseModel, conlist
# from typing import Optional
# from libs.utils.logging import AzureTableHandler

# bp = Blueprint()

# NOTE : This function is on hold because the Sales Uploader is the new "starter" entrypoint for sales uploads.

# # initialize logging features
# __handler = AzureTableHandler()
# __logger = logging.getLogger("matchback.logger")
# if __handler not in __logger.handlers:
#     __logger.addHandler(__handler)


# @bp.route(route="esquire/matchback/starter", methods=["POST"])
# @bp.durable_client_input(client_name="client")
# async def starter_matchback(req: HttpRequest, client: DurableOrchestrationClient):
#     logger = logging.getLogger("matchback.logger")

#     # load the request payload as a Pydantic object
#     payload = MatchbackPayload.model_validate_json(req.get_body()).model_dump()

#     logging.warning(payload)

#     return HttpResponse(status_code=200)

# class ClientInfo(BaseModel):
#     driveId: str
#     groupName: str
#     directoryName: str
#     dateName: str

# class salesFileHeaders(BaseModel):
#     address: str
#     city: str
#     state: str
#     zipcode: str
#     date: str
#     sale_amount: Optional[str]

# class salesInfo(BaseModel):
#     salesFiles: conlist(str, min_length=1)
#     headers : salesFileHeaders

# class matchbackSettings(BaseModel):
#     targetRecallMonths: Optional[int] = 11
#     exportName: str

# class matchbackOptions(BaseModel):
#     breakdownColumns: Optional[list[str]] = []
#     multiBreakdownPairs: Optional[list[tuple[str]]] = []
#     removeBlacklist: Optional[bool] = False
#     uniqueHouseholds: Optional[bool] = False
#     OTTBreakdown: Optional[bool] = False
#     competitorsByAudience: Optional[bool] = False
#     jeromes: Optional[bool] = False
#     suppressSQL: Optional[bool] = False
#     lookbackActive: Optional[bool] = False
#     opportunityMatchback: Optional[bool] = False

# class MatchbackPayload(BaseModel):
#     """
#     This class ingests and validates the payload elements for the Matchback report, with type enforcement.
#     """
#     clientInfo:ClientInfo
#     salesInfo:salesInfo
#     settings:matchbackSettings
#     options:matchbackOptions