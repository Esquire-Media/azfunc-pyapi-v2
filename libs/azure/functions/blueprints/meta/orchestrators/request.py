# File: libs/azure/functions/blueprints/meta/orchestrators/request.py

# from aiopenapi3 import ResponseSchemaError
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint
import json, logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def meta_orchestrator_request(
    context: DurableOrchestrationContext,
):
    retry = RetryOptions(15000, 5)
    ingress = context.get_input()
    after = None
    page = 0
    data = []
    schema_retry = 0
    while True:
        if schema_retry > 3:
            context.set_custom_status(
                f"Too many retries for Operation {ingress['operationId']}."
            )
            break
        try:
            context.set_custom_status(
                f"Requesting page {page} for Operation {ingress['operationId']}."
            )
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
        except Exception as e:
            logging.error(e)
            schema_retry += 1
            continue
        if response:
            if "error" in response.keys():
                match response["error"]["code"]:
                    case 4 | 17 | 80004:
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
                                f"Waiting to get page {page}. Throttled until {timer.isoformat()}."
                            )
                            yield context.create_timer(timer)
                            continue
                    case 10:
                        # Permissions error
                        break
                    case _:
                        # https://developers.facebook.com/docs/marketing-api/error-reference/
                        # 1  : Unknown Error
                        # 100: Invalid parameter
                        # 102: Session key invalid or no longer valid
                        # 190: Invalid OAuth 2.0 Access Token
                        # 368: The action attempted has been deemed abusive or is otherwise disallowed
                        raise Exception(
                            "{} ({}): {}".format(
                                response["error"]["message"],
                                response["error"]["code"],
                                response["error"].get("error_user_msg", ""),
                            )
                        )
            else:
                if response["data"]:
                    if isinstance(response["data"], list):
                        data += response["data"]
                    else:
                        data.append(response["data"])

                if ingress.get("recursive", False) and response["next"]:
                    after = response["next"]
                    page += 1
                    continue
                else:
                    context.set_custom_status(f"All requests completed.")
        break

    if ingress.get("return", True) or ingress.get("destination", {}):
        if len(data) == 1:
            return data[0]
        return data
    return []
