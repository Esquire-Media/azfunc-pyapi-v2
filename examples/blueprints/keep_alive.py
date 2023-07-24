from azure.functions import TimerRequest
from libs.azure.functions import Blueprint

bp = Blueprint()

@bp.timer_trigger(arg_name="timer", schedule="0 */5 * * * *")
def timer(timer: TimerRequest):
    pass