# File: /libs/azure/functions/blueprints/esquire/audiences/egress/xandr/activities/getSegment.py

from azure.durable_functions import Blueprint
from libs.openapi.clients import XandrAPI
import logging

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceXandr_getSegment(ingress: str):
    XA = XandrAPI(asynchronus=False)
    factory = XA.createRequest("GetSegment")
    
    _, data, _ = factory.request(parameters={"id": ingress})
    
    if data.response.segment:
        if data.response.segment.state:
            return data.response.segment.state
    
    return ""