from azure.durable_functions import DurableEntityContext
from libs.azure.functions import Blueprint

bp = Blueprint()


@bp.entity_trigger(context_name="context")
def entity_generic(context: DurableEntityContext):
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
