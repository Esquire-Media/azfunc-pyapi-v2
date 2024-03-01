from libs.azure.functions import Blueprint
from libs.data import from_bind
import json
from json_logic import jsonLogic
import logging

bp = Blueprint()


# activity to fill in the geo data for each audience object
@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_fetchAudienceDatasource(ingress: dict):
    provider = from_bind("keystone")

    tables = provider.models["public"]
    session = provider.connect()

    audiences = [
        [
            r.id,
            r.dataSource,
            jsonlogic_to_sql(r.dataFilter)
        ]
        for r in session.query(
            tables["Audience"].id,
            tables["Audience"].dataSource,
            tables["Audience"].dataFilter,
        ).all()
    ]

    return audiences

def jsonlogic_to_sql(jsonlogic):
    def parse_logic(logic):
        if "and" in logic:
            return "(" + " AND ".join(map(parse_logic, logic["and"])) + ")"
        elif "or" in logic:
            return "(" + " OR ".join(map(parse_logic, logic["or"])) + ")"
        elif "<=" in logic:
            left, right = logic["<="][0], logic["<="][2]
            var_name = logic["<="][1]["var"]
            return f"({var_name} BETWEEN '{left}' AND '{right}')"
        elif ">=" in logic:
            var_name = logic[">="][0]["var"]
            value = logic[">="][1]
            return f"({var_name} >= {value})"
        elif "==" in logic:
            var_name = logic["=="][0]["var"]
            value = logic["=="][1]
            return f"({var_name} = '{value}')"
        elif "in" in logic:
            var_name = logic["in"][0]["var"]
            values = ",".join([f"'{value}'" for value in logic["in"][1]])
            return f"({var_name} IN ({values}))"
        else:
            raise ValueError("Unsupported operation")

    logic_dict = json.loads(jsonlogic)
    return parse_logic(logic_dict)