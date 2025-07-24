
from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy import update
from sqlalchemy.orm import Session

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesBuilder_findNeighbors(ingress: dict):
    pass