from azure.functions import Blueprint, TimerRequest

bp = Blueprint()


@bp.timer_trigger(arg_name="timer", schedule="0 */5 * * * *")
def timer(timer: TimerRequest):
    pass
