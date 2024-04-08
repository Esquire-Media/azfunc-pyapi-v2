from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import joinedload
import orjson

bp = Blueprint()


# activity to fill in the geo data for each audience object
@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_fetchAudienceDatasource(ingress: dict):
    provider = from_bind("keystone")
    audience = provider.models["public"]["Audience"]
    session = provider.connect()
    audiences = []

    stmt = select(audience).options(joinedload(audience.related_DataSource))

    for row in session.scalars(stmt):
        audiences.append(
            (
                row.id,
                row.related_DataSource.id,
                jsonlogic_to_sql(row.dataFilter),
            )
        )

    return audiences


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
                return f"{logic['var']}"

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
    return f"WHERE {parse_logic(logic)}"
