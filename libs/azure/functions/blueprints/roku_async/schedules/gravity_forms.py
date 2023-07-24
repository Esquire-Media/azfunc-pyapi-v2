# File: libs/azure/functions/blueprints/schedules/report.py

from azure.durable_functions import DurableOrchestrationClient
from azure.functions import TimerRequest
from libs.azure.functions.blueprints.roku_async.helpers import (
    TABLE_CLIENTS,
    request_initializer,
)
from libs.azure.functions import Blueprint
from libs.openapi.clients.gravity_forms import GravityFormsAPI

bp = Blueprint()

# Mapping o country codes
COUNTRY_MAP = {
    10: {
        "12.1": "15",
        "12.2": "16",
        "12.3": "17",
        "12.4": "18",
        "12.5": "19",
        "12.6": "20",
        "12.7": "21",
        "12.8": "22",
        "12.9": "23",
        "12.11": "24",
        "12.12": "25",
        "12.13": "26",
        "12.14": "27",
        "12.15": "28",
        "12.16": "29",
        "12.17": "30",
        "12.18": "31",
        "12.19": "32",
        "12.21": "33",
        "12.22": "34",
        "12.23": "35",
        "12.24": "36",
        "12.25": "37",
        "12.26": "38",
        "12.27": "39",
        "12.28": "40",
        "12.29": "41",
        "12.31": "42",
        "12.32": "43",
        "12.33": "44",
        "12.34": "45",
        "12.35": "46",
        "12.36": "47",
        "12.37": "48",
        "12.38": "49",
        "12.39": "50",
        "12.41": "51",
    }
}


@bp.logger()
@bp.timer_trigger(arg_name="timer", schedule="0 */5 * * * * ")
@bp.durable_client_input(client_name="client")
async def roku_async_schedule_gravity_forms(
    timer: TimerRequest, client: DurableOrchestrationClient
) -> None:
    """
    Asynchronous method for scheduling gravity forms with Azure Durable Functions.

    This method is designed to run every 5 minutes (according to the schedule
    parameter in timer_trigger) and initialize and handle gravity forms for
    each client entity.

    Parameters
    ----------
    timer : TimerRequest
        The TimerRequest object containing the timing details for the scheduled task.
    client : DurableOrchestrationClient
        The client context for Durable Functions.

    Returns
    -------
    None
    """

    for agency in TABLE_CLIENTS["agencies"].list_entities():
        if agency.get("GravityForms_FormID", None):
            current_page = 0
            api = GravityFormsAPI(url="https://esquireadvertising.com/wp-json")
            req = api.createRequest(("/forms/{formId}/entries", "get"))
            while True:
                headers, data, response = await req.request(
                    parameters={
                        "formId": agency["GravityForms_FormID"],
                        "sorting": {
                            "key": "date_created",
                            "direction": "DESC",
                        },
                        "paging": {
                            "page_size": 10,
                            "current_page": (current_page := current_page + 1),
                        },
                    }
                )
                value = response.json()
                if value["entries"]:
                    for entry in value["entries"]:
                        try:
                            TABLE_CLIENTS["submissions"].get_entity(
                                str(agency["GravityForms_FormID"]), str(entry["id"])
                            )
                            break
                        except:
                            payload = {
                                "advertiser": {
                                    "tenant": "4eb95564-75e6-4b66-81f3-28b3d64567dd",
                                    "id": entry["14"],
                                    "name": entry["13"],
                                    "domain": entry["53"],
                                    "category": entry["55"],
                                },
                                "date_range": {
                                    "start": entry["3"],
                                    "months": entry["8"],
                                },
                                "budget": {
                                    "monthly_impressions": int(entry["6"]),
                                    "cpm_client": int(entry["6"])
                                    / 1000
                                    / agency["CPM_Client"],
                                    "cpm_tenant": int(entry["6"])
                                    / 1000
                                    / agency["CPM_Tenant"],
                                },
                                "creative": entry["7"],
                                "landing_page": entry["58"],
                                "targeting": [
                                    {
                                        "type": "broad",
                                        "base": {
                                            "country": isocode.strip(),
                                            "zips": [
                                                zipcode.strip()
                                                for zipcode in entry[
                                                    COUNTRY_MAP[
                                                        agency["GravityForms_FormID"]
                                                    ][key]
                                                ].split(",")
                                            ],
                                        },
                                    }
                                    for key, isocode in entry.items()
                                    if key.startswith("12.")
                                    if isocode
                                ],
                            }
                            TABLE_CLIENTS["submissions"].create_entity(
                                {
                                    "PartitionKey": str(agency["GravityForms_FormID"]),
                                    "RowKey": str(entry["id"]),
                                }
                            )

                            # Initialize the request and get the instance id of the started task
                            instanceId = await request_initializer(
                                request=payload, client=client
                            )
                else:
                    break
