from .. import app
from libs.azure.functions import HttpRequest


# An HTTP-Triggered Function with a Durable Functions Client binding
@app.route(route="orchestrators/simple")
@app.durable_client_input(client_name="client")
async def simple_start(req: HttpRequest, client):
    instance_id = await client.start_new("simple_root")
    response = client.create_check_status_response(req, instance_id)
    return response


# Orchestrator
@app.orchestration_trigger(context_name="context")
def simple_root(context):
    result1 = yield context.call_activity("simple_hello", "Seattle")
    result2 = yield context.call_activity("simple_hello", "Tokyo")
    result3 = yield context.call_activity("simple_hello", "London")

    return [result1, result2, result3]


# Activity
@app.activity_trigger(input_name="city")
def simple_hello(city: str):
    return "Hello " + city
