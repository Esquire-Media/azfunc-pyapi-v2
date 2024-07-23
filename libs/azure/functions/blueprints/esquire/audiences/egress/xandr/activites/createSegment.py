# File: /libs/azure/functions/blueprints/esquire/audiences/egress/xandr/activities/createSegment.py

from azure.durable_functions import Blueprint
from libs.openapi.clients import XandrAPI

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceXandr_createSegment(ingress: dict):
    XA = XandrAPI(asynchronus=False)
    factory = XA.createRequest("CreateSegment")
    
    _, data, _ = factory.request(**ingress)
    
    if data.response.segment:
        if data.response.segment.id:
            return data.response.segment.id
    
    raise Exception(data)