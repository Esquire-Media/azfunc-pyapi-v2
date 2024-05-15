from dateutil.relativedelta import relativedelta
import datetime
try:
    import orjson as json
except:
    import json

def extract_dates(request, now = datetime.datetime.now()):
    start_expr = request['dateStart']['date_add']
    start_date = now + relativedelta(**{start_expr[2]:start_expr[1]})
    end_expr = request['dateEnd']['date_add']
    end_date = now + relativedelta(**{end_expr[2]:end_expr[1]})
    
    return start_date, end_date

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

    logic = json.loads(json_logic) if isinstance(json_logic, str) else json_logic
    return parse_logic(logic)
