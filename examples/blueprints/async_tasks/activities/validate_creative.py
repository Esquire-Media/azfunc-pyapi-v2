from libs.azure.functions import Blueprint
from libs.utils.helpers import mp4_metadata
    

bp = Blueprint()


# Activity
@bp.activity_trigger(input_name="url")
def validate_creative(url: str):
    
    return header