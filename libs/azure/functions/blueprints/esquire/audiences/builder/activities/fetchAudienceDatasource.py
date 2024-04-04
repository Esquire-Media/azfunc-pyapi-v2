from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import joinedload
import json

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
            (row.id, row.related_DataSource.id, row.related_DataSource.title, row.dataFilter, jsonlogic_to_sql(row.dataFilter))
        )

    return audiences


def jsonlogic_to_sql(jsonlogic):
    def parse_logic(logic):
        if "and" in logic:
            return "(" + " AND ".join(map(parse_logic, logic["and"])) + ")"
        elif "or" in logic:
            return "(" + " OR ".join(map(parse_logic, logic["or"])) + ")"
        elif "<=" in logic: #
            var_name = logic["<="][0]["var"]
            value = logic["<="][1]
            sql = f"({var_name} <= {value})"
            return sql
        elif ">=" in logic:
            var_name = logic[">="][0]["var"]
            value = logic[">="][1]
            sql = f"({var_name} => {value})"
            return sql
        elif "==" in logic:
            var_name = logic["=="][0]["var"]
            value = logic["=="][1]
            return f"({var_name} = '{value}')"
        elif "!=" in logic:
            var_name = logic["!="][0]["var"]
            value = logic["!="][1]
            return f"({var_name} != '{value}')"
        elif "<" in logic:
            var_name = logic["<"][0]["var"]
            value = logic["<"][1]
            return f"({var_name} < '{value}')"
        elif ">" in logic:
            var_name = logic[">"][0]["var"]
            value = logic[">"][1]
            return f"({var_name} > '{value}')"
        elif "in" in logic:
            var_name = logic["in"][0]["var"]
            values = ",".join([f"'{value}'" for value in logic["in"][1]])
            return f"({var_name} IN ({values}))"
        else:
            raise ValueError("Unsupported operation")

    logic_dict = json.loads(jsonlogic)
    return parse_logic(logic_dict)
