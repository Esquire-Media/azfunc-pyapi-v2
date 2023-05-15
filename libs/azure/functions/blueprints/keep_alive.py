from libs.azure.functions import Blueprint
from azure.functions import TimerRequest

bp = Blueprint()

@bp.timer_trigger("timer", schedule="0 */5 * * * *")
def keep_alive(timer: TimerRequest):
    pass