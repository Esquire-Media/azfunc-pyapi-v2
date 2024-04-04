from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import joinedload

# from json_logic import jsonLogic
import logging

bp = Blueprint()


# activity to fill in the geo data for each audience object
@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_fetchAudienceProcesses(ingress: dict):
    provider = from_bind("keystone")
    session = provider.connect()

    provider = from_bind("keystone")
    audience = provider.models["public"]["Audience"]
    datasource = provider.models["public"]["DataSource"]
    # processingstep = provider.models["public"]["ProcessingStep"]

    session = provider.connect()

    stmt = (
        select(audience)
        .options(
            joinedload(audience.related_DataSource),
            joinedload(audience.related_ProcessingStep),
        )
        .where(audience.id == ingress[0])
    )

    for row in session.scalars(stmt):
        logging.warning(
            row.id
            + " "
            + row.related_DataSource.title
            + " "
            + row.related_ProcessingStep.outputType,
        )

    return {}
    # get the audience processing step
    datasources = [
        # r.id
        # r.dataSource,
        r.dataType
        # r.outputType,
        for r in session.query(
            tables["Audience"].id,
            tables["Audience"].dataSource,
            tables["DataSource"].dataType,
            tables["ProcessingStep"].outputType,
        )
        .join(
            tables["DataSource"],
            tables["Audience"].dataSource == tables["DataSource"].id,
        )
        .join(
            tables["ProcessingStep"],
            tables["Audience"].id == tables["ProcessingStep"].audience,
        )
        .filter(tables["Audience"].id == ingress[0])
    ]

    logging.warning(datasources)

    return datasources
