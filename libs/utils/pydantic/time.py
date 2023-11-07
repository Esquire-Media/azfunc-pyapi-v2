from libs.utils.pydantic.geometry import Latitude, Longitude
from pydantic import BaseModel
from pydantic import AfterValidator, ValidationError
from typing import Annotated
import re
from datetime import date

def check_date_format(x:str):
    x = x.strip()
    try:
        return x
    except:
        raise ValidationError(f"String '{x}' could not be interpreted as an isoformat date.")
Date = Annotated[str, AfterValidator(check_date_format)]