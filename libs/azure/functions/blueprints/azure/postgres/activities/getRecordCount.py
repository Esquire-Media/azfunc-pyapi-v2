# File: libs/azure/functions/blueprints/postgres/activities/getRecordCount.py

from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
import logging

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_azurePostgres_getRecordCount(ingress: dict) -> int:
    # ingress = {
    #     "bind": "BIND_HANDLE",
    #     "query": "SELECT * FROM table"
    # }
    # Establish a session with the database using the provided bind information.
    session: Session = from_bind(ingress["bind"]).connect()
    result = session.execute(text("SELECT COUNT(*) FROM ({}) AS total".format(ingress["query"]))).one_or_none()
    
    return result[0]