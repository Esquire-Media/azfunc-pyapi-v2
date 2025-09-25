# File: /libs/azure/functions/blueprints/esquire/audiences/builder/utils.py

from dateutil.relativedelta import relativedelta
import datetime, orjson as json


def extract_dates(request, now=datetime.datetime.now()):
    """
    Extracts start and end dates from a request using relative delta.

    Parameters:
    request (dict): A dictionary containing 'dateStart' and 'dateEnd' keys with 'date_add' sub-keys that define the date calculation.
    now (datetime.datetime, optional): The base datetime to use for calculations. Defaults to the current datetime.

    Returns:
    tuple: A tuple containing the calculated start_date and end_date.
    """
    start_expr = request["dateStart"]["date_add"]
    start_date = now + relativedelta(**{start_expr[2]: start_expr[1]})
    end_expr = request["dateEnd"]["date_add"]
    end_date = now + relativedelta(**{end_expr[2]: end_expr[1]})

    return start_date, end_date


def jsonlogic_to_sql(json_logic):
    """
    Converts JSON Logic into an SQL WHERE clause, adjusted for MSSQL.

    Parameters:
    json_logic (str or dict): The JSON Logic structure to convert. It can be a JSON string or a dictionary.

    Returns:
    str: The resulting SQL WHERE clause.

    Raises:
    ValueError: If the JSON Logic structure contains unsupported operations or types.
    """

    def parse_logic(logic):
        # Check if logic is directly a dictionary with an operation
        if isinstance(logic, dict):
            if "and" in logic:
                conditions = [parse_logic(sub_logic) for sub_logic in logic["and"]]
                return " AND ".join(f"({condition})" for condition in conditions)
            
            if "or" in logic:
                conditions = [parse_logic(sub_logic) for sub_logic in logic["or"]]
                return " OR ".join(f"({condition})" for condition in conditions)

            elif ">" in logic:
                left = parse_logic(logic[">"][0])
                right = parse_logic(logic[">"][1])
                return f"{left} > {right}"

            elif ">=" in logic:
                left = parse_logic(logic[">="][0])
                right = parse_logic(logic[">="][1])
                return f"{left} >= {right}"

            elif "==" in logic:
                left = parse_logic(logic["=="][0])
                right = parse_logic(logic["=="][1])
                return f"{left} = {right}"

            elif "<" in logic:
                left = parse_logic(logic["<"][0])
                right = parse_logic(logic["<"][1])
                return f"{left} > {right}"

            elif "<=" in logic:
                left = parse_logic(logic["<="][0])
                right = parse_logic(logic["<="][1])
                return f"{left} >= {right}"

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

    logic = json.loads(json_logic) if isinstance(json_logic, str) else json_logic
    return parse_logic(logic)

def extract_fields_from_dataFilter(dataFilter):
    """
    from a given sql-ified datafilter, it tries to find all of the fields that we're interacting with
    """
    import re
    return re.findall(r'"([^"]+)"', dataFilter)

def extract_tenant_id_from_datafilter(sql):
    import re
    match = re.search(r'"tenant_id"\s*(?:=|!=|<>|<|>|LIKE|IN)\s*\'([^\']+)\'', sql, re.IGNORECASE)
    return match.group(1) if match else None

def extract_daysback_from_dataFilter(sql):
    import re
    match = re.search(
        r'"days_back"\s*(?:=|!=|<>|<|>|LIKE|IN)\s*(?:\'([^\']+)\'|(\d+))',
        sql,
        re.IGNORECASE
    )
    val = match.group(1) or match.group(2) if match else None

    return float(val)