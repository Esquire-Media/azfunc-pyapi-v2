import requests
import csv
from io import StringIO
from azure.durable_functions import Blueprint

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_extractPartitions(ingress: dict) -> list[str]:

    url = ingress["url"]
    
    response = requests.get(url)
    response.raise_for_status()

    seen = set()
    out = []
    csv_file = StringIO(response.text)
    reader = csv.DictReader(csv_file)

    for row in reader:
        city = row.get("city")
        state = row.get("state")
        zip_code = row.get("zipCode")
        if city and state and zip_code:
            key = (city.lower().strip(), state.upper().strip(), zip_code.strip())
            if key not in seen:
                out.append({"city": city, "state": state, "zip": zip_code})
                seen.add(key)


    return out