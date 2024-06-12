# File: libs/azure/functions/blueprints/synapse/cetas.py

from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
async def activity_synapse_queryTables(ingress: dict):
    """
    This function queries the system objects to find Synapse tables matching a given string pattern.

    Params
    bind        Bind that specifies the Synapse database to connect to.
    pattern     SQL string pattern to match against (e.g. "myTable_%")
    """

    # connect to sqlalchemy session
    session: Session = from_bind(ingress["bind"]).connect()

    # build and execute query to search for tables
    query = f"""
    SELECT
        [name]
    FROM sys.objects
    WHERE [type] = N'U'
    AND [name] LIKE '{ingress['pattern']}'  
    """
    res = session.execute(text(query)).all()

    # return table names in a flat list
    return [item[0] for item in res]