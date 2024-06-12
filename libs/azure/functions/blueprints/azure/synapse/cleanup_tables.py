# File: libs/azure/functions/blueprints/synapse/cetas.py

from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
async def activity_synapse_cleanupTables(ingress: dict):
    """
    This function cleans up the system objects left behind by unwanted Synapse tables.

    Params
    bind            Bind that specifies the Synapse database to connect to.
    table_names     List of table names to drop.
    schema          [Optional] table schema name. Default is "dbo".
    """

    # connect to sqlalchemy session
    session: Session = from_bind(ingress["bind"]).connect()

    # load optional param and apply default value if it doesn't exist
    schema = ingress.get("schema", "dbo")

    for table_name in ingress['table_names']:
        # build and execute query to search for tables
        query = f"""
        DROP EXTERNAL TABLE [{schema}].[{table_name}] 
        """
        session.execute(text(query))
    session.commit()

    # return table names in a flat list
    return True