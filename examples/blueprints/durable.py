from azure.durable_functions import (
    DurableOrchestrationClient,
    DurableOrchestrationContext,
    EntityId,
    DurableEntityContext,
)
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest
import logging
import uuid

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()


# Define an HTTP-triggered function that starts a new orchestration
@bp.route(route="orchestrators/simple")
@bp.durable_client_input(client_name="client")
async def example_start(req: HttpRequest, client: DurableOrchestrationClient):
    # Generate a unique instance ID for the orchestration
    instance_id = str(uuid.uuid4())

    # Signal a Durable Entity to instantiate and set its state
    await client.signal_entity(
        entityId=EntityId(name="example_entity", key=f"{instance_id}"),
        operation_name="add",
        operation_input=5,
    )

    # Start a new instance of the orchestrator function
    await client.start_new(
        orchestration_function_name="example_orchestrator",
        instance_id=instance_id,
    )

    # Return a response that includes the status query URLs
    return client.create_check_status_response(req, instance_id)


# Define a Durable Entity function that maintains a counter
@bp.entity_trigger(context_name="context")
def example_entity(context: DurableEntityContext):
    # Get the current state of the entity, or initialize it to 0
    current_value = context.get_state(lambda: 0)

    # Perform operations based on the operation name
    operation = context.operation_name
    if operation == "add":
        # Add the input value to the current state
        amount = context.get_input()
        current_value += amount
    elif operation == "get":
        # Return the current state as the result
        context.set_result(current_value)

    # Update the state of the entity
    context.set_state(current_value)


"""
Orchestrator functions must be written to be idempotent. This means that they
should produce the same result if they are run multiple times with the same input.
This is important because the Durable Task Framework replays orchestrator functions
multiple times. Non-deterministic operations, such as generating random numbers or
calling non-deterministic functions, should be avoided in orchestrator functions.
"""
@bp.orchestration_trigger(context_name="context")
def example_orchestrator(context: DurableOrchestrationContext):
    # Create an EntityId for the Durable Entity
    entity_id = EntityId("example_entity", context.instance_id)

    # Get the current value of the counter
    counter_value = yield context.call_entity(entity_id, "get")

    # Increment the counter until it reaches 10
    while counter_value < 10:
        logging.warn(counter_value)
        yield context.call_activity("example_activity", counter_value)
        counter_value = yield context.call_entity(entity_id, "get")

    # Call an activity function with the final counter value
    yield context.call_activity("example_activity", counter_value)

    return ""


# Define an activity function that logs a message
@bp.activity_trigger(input_name="message")
@bp.durable_client_input(client_name="client")
def example_activity(message: str, client: DurableOrchestrationClient):
    logging.warn(f"Counted to: {message}")
    return ""
