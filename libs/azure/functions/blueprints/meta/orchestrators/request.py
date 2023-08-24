# File: libs/azure/functions/blueprints/meta/orchestrators/request.py

from aiopenapi3 import ResponseSchemaError
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint
import json

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def meta_orchestrator_request(
    context: DurableOrchestrationContext,
):
    retry = RetryOptions(15000, 5)
    ingress = context.get_input()
    after = None
    data = []
    schema_retry = 0
    while True:
        if schema_retry > 3:
            break
        try:
            context.set_custom_status("")
            response = yield context.call_activity_with_retry(
                "meta_activity_request",
                retry,
                {
                    **ingress,
                    "parameters": {
                        **ingress["parameters"],
                        **({"after": after} if after else {}),
                    },
                },
            )
        except ResponseSchemaError:
            schema_retry += 1
            continue
        if response:
            if "error" in response["data"].keys():
                match response["data"]["error"]["code"]:
                    case 200:
                        # Permissions error
                        break
                    case 80004:
                        if throttle := (
                            max(
                                [
                                    a["estimated_time_to_regain_access"]
                                    for t in json.loads(
                                        response["headers"]["X-Business-Use-Case-Usage"]
                                    ).values()
                                    for a in t
                                ]
                            )
                            if "X-Business-Use-Case-Usage" in response["headers"].keys()
                            else 0
                        ):
                            timer = datetime.utcnow() + timedelta(minutes=throttle)
                            context.set_custom_status(
                                f"Throttled until {timer.isoformat()}."
                            )
                            yield context.create_timer(timer)
                            continue
                    case _:
                        # 100: Invalid parameter
                        # 190: Invalid OAuth 2.0 Access Token
                        # 368: The action attempted has been deemed abusive or is otherwise disallowed
                        raise Exception(
                            "{}: {}".format(
                                response["data"]["error"]["message"],
                                response["data"]["error"].get("error_user_msg", ""),
                            )
                        )

            if ingress.get("return", True):
                if "data" in response["data"]:
                    if response["data"]["data"] != None:
                        data.append(response["data"]["data"])
                    else:
                        data.append(response["data"])
                else:
                    data.append(response["data"])

            if (
                ingress.get("recursive", False)
                and "paging" in response["data"].keys()
                and isinstance(response["data"]["paging"], dict)
                and "next" in response["data"]["paging"].keys()
                and response["data"]["paging"]["next"] != None
            ):
                after = response["data"]["paging"]["cursors"]["after"]
                continue

        break

    if ingress.get("return", True):
        if isinstance(data[0], list):
            return [d for l in data for d in l]
        return data
    return []
