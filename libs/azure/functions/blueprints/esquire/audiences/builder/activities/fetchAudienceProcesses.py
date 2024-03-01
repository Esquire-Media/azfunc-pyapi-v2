from libs.azure.functions import Blueprint
from libs.data import from_bind
import json
from json_logic import jsonLogic
import logging

bp = Blueprint()


# activity to fill in the geo data for each audience object
@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_fetchAudienceProcesses(ingress: dict):
    provider = from_bind("keystone")

    tables = provider.models["public"]
    session = provider.connect()

    # get the audiences
    datasources = [
        [
            r.id,
            r.dataSource,
            r.dataType,
            r.title,
            r.B,
            r.outputType,
        ]
        for r in session.query(
            tables["Audience"].id,
            tables["Audience"].dataSource,
            tables["DataSource"].dataType,
            tables["DataType"].title,
            tables["_Audience_processes"].B,
            tables["ProcessingStep"].outputType,
        )
        .join(
            tables["DataSource"],
            tables["Audience"].dataSource == tables["DataSource"].id,
        )
        .join(
            tables["DataType"],
            tables["DataSource"].dataType == tables["DataType"].id,
        )
        .join(
            tables["_Audience_processes"],
            tables["Audience"].id == tables["_Audience_processes"].A,
        )
        .join(
            tables["ProcessingStep"],
            tables["_Audience_processes"].B == tables["ProcessingStep"].id,
        )
        .all()
    ]

    # examples output
    [
        [
            "clt318ik2000ft86cgsnpghif",  # Audience ID
            "clt318h5e0009t86c8ayrprkp",  # DataSource ID
            "clt318g970005t86ctx76g2ux",  # DataType ID
            "Polygons",  # DataType Title
            "clt6badqj0002t80glq4iut46", # Processing Step ID
            "clt318g390004t86cxigmcnfa", # Output Type
        ],
        [
            "clt318ik2000ft86cgsnpghif",
            "clt318h5e0009t86c8ayrprkp",
            "clt318g970005t86ctx76g2ux",
            "Polygons",
            "clt6dq82d0003t80gz8jj03wf",
            "clt318fwa0003t86cftmzzl6q",
        ],
    ]

    return datasources
