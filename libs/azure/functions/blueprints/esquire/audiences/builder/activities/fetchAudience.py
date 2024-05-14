# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/fetchAudience.py.py

from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session, lazyload
import orjson

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_fetchAudience(ingress: dict):
    provider = from_bind("keystone")
    audience = provider.models["public"]["Audience"]

    session: Session = provider.connect()
    query = (
        select(audience)
        .options(
            lazyload(audience.related_Advertiser),
            lazyload(audience.related_DataSource),
            lazyload(audience.collection_ProcessingStep),
        )
        .where(audience.id == ingress["id"])
    )
    result = session.execute(query).one_or_none()

    if result:
        return {
            **ingress,
            "advertiser": {
                "id": result.Audience.related_Advertiser.id,
                "meta": result.Audience.related_Advertiser.meta,
                "oneview": result.Audience.related_Advertiser.oneview,
                "xandr": result.Audience.related_Advertiser.xandr,
            },
            "status": result.Audience.status,
            "rebuild": result.Audience.rebuild,
            "rebuildUnit": result.Audience.rebuildUnit,
            "TTL_Length": result.Audience.TTL_Length,
            "TTL_Unit": result.Audience.TTL_Unit,
            "dataSource": {
                "id": result.Audience.related_DataSource.id,
                "dataType": result.Audience.related_DataSource.dataType,
            },
            "dataFilter": jsonlogic_to_sql(result.Audience.dataFilter),
            "processes": list(
                map(
                    lambda row: {
                        "id": row.id,
                        "sort": row.sort,
                        "outputType": row.outputType,
                        "customCoding": row.customCoding,
                    },
                    result.Audience.collection_ProcessingStep,
                )
            ),
        }
    return ingress


def jsonlogic_to_sql(json_logic):
    """
    Converts JSON Logic into an SQL WHERE clause, adjusted for MSSQL.
    """

    def parse_logic(logic):
        # Check if logic is directly a dictionary with an operation
        if isinstance(logic, dict):
            if "and" in logic:
                conditions = [parse_logic(sub_logic) for sub_logic in logic["and"]]
                return " AND ".join(f"({condition})" for condition in conditions)

            elif ">=" in logic:
                left = parse_logic(logic[">="][0])
                right = parse_logic(logic[">="][1])
                return f"{left} >= {right}"

            elif "==" in logic:
                left = parse_logic(logic["=="][0])
                right = parse_logic(logic["=="][1])
                return f"{left} = {right}"

            elif "in" in logic:
                var = parse_logic(logic["in"][0])
                values = ", ".join(f"'{value}'" for value in logic["in"][1])
                return f"{var} IN ({values})"

            elif "var" in logic:
                return f"\"{logic['var']}\""

            elif "date_add" in logic:
                base = parse_logic(logic["date_add"][0])
                offset = logic["date_add"][1]
                unit = logic["date_add"][2]
                # Adjusting for MSSQL's DATEADD function
                date_part = {
                    "day": "day",
                    "month": "month",
                    "year": "year",
                    "days": "day",  # Handling plural forms
                    "months": "month",
                    "years": "year",
                }[unit]
                return f"DATEADD({date_part}, {offset}, {base})"

            elif "now" in logic:
                # Using GETDATE() for the current timestamp in MSSQL
                return "GETDATE()"
        # Direct handling for non-dict types (e.g., when logic is a part of a larger operation)
        elif isinstance(logic, str):
            return f"'{logic}'"
        elif isinstance(logic, int):
            return str(logic)
        else:
            raise ValueError(f"Unsupported operation or type: {type(logic)}")

    logic = orjson.loads(json_logic) if isinstance(json_logic, str) else json_logic
    return parse_logic(logic)
