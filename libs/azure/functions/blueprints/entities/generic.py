from azure.durable_functions import DurableEntityContext
from libs.azure.functions import Blueprint

# Create a Blueprint instance
bp = Blueprint()

@bp.entity_trigger(context_name="context")
def entity_generic(context: DurableEntityContext):
    """
    Entity trigger function for generic entities.

    Parameters
    ----------
    context : DurableEntityContext
        The durable entity context.

    Notes
    -----
    This entity trigger function is responsible for handling operations on generic entities.
    It receives a durable entity context that provides methods for interacting with the entity's state.

    Steps:
    1. Get or initialize the current state of the entity.
    2. Cache the input data to a variable.
    3. Based on the operation name:
        - If it is "init" and the input is a dictionary, replace the entire current state with the input.
        - If it matches a key in the current state, replace the value of that key with the input.
        - If it is "update" and the input is a dictionary, replace or add multiple key-value pairs to the current state.
        - Otherwise, set the results to the current state.
    4. Set the current state to the processed value.

    This function can be customized to handle other operations and modify the entity's state based on the application's requirements.
    """
    # Get or initialize the current state
    value = context.get_state(dict)

    # Cache the input data to a variable
    input_ = context.get_input()

    # Replace the entire current state
    if context.operation_name == "init" and isinstance(input_, dict):
        value = input_
    # Replace only the value of a single key
    elif context.operation_name in value.keys():
        value[context.operation_name] = context.get_input()
    # Replace or add multiple key:value pairs
    elif context.operation_name == "update" and isinstance(input_, dict):
        value = {**value, **input_}
    # Set the results
    else:
        context.set_result(value)

    # Set the current state to the processed value
    context.set_state(value)
