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
    
    # possible intervention if there is custom logic before the final parse
    if contains_custom_field(logic):
        logic = rewrite_custom_fields_json(logic)

    return parse_logic(logic)

def rewrite_custom_fields_json(json_logic):
    """
    Function for handling custom datafilter fields. This will take a custom set of filters and turn them into one
    EG:
    {'and': [{'==': [{'var': 'custom.field'}, 'Sealy']},
    {'>': [{'var': 'custom.numeric_value'}, 0]}]}]}
    becomes
    {'and': [{'>': [{'var': 'Sealy'}, 0]}]}]}
    """
    def process(node):
        if isinstance(node, dict):
            if 'and' in node:
                # Check if this group has both a custom.field and value usage
                field = None
                new_ands = []
                for cond in node['and']:
                    if isinstance(cond, dict) and '==' in cond:
                        left, right = cond['==']
                        if isinstance(left, dict) and left.get('var') == 'custom.field':
                            field = right
                            continue  # drop this condition
                    new_ands.append(cond)

                # Replace any custom.value with the field
                if field:
                    updated = []
                    for cond in new_ands:
                        updated.append(replace_custom_value(cond, field))
                    return {'and': updated}
                else:
                    return {'and': [process(c) for c in node['and']]}
            else:
                return {k: process(v) for k, v in node.items()}
        elif isinstance(node, list):
            return [process(i) for i in node]
        else:
            return node

    def replace_custom_value(cond, field):
        if isinstance(cond, dict):
            for op, args in cond.items():
                new_args = []
                for arg in args:
                    if isinstance(arg, dict) and arg.get('var') in ('custom.numeric_value', 'custom.text_value'):
                        new_args.append({'var': field})
                    else:
                        new_args.append(arg)
                return {op: new_args}
        return cond

    return process(json_logic)

def contains_custom_field(node):
    if isinstance(node, dict):
        for key, val in node.items():
            if key == '==':
                left, _ = val
                if isinstance(left, dict) and left.get('var') == 'custom.field':
                    return True
            elif isinstance(val, (dict, list)):
                if contains_custom_field(val):
                    return True
    elif isinstance(node, list):
        return any(contains_custom_field(i) for i in node)
    return False

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