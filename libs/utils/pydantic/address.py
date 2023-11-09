from libs.utils.pydantic.geometry import Latitude, Longitude
from pydantic import BaseModel
from pydantic import AfterValidator, ValidationError
from typing import Annotated
import re

# define a few custom Pydantic data types to assist in validating each of the payload fields
class AddressComponents(BaseModel):
    address: str
    city: str
    state: str
    zip: str

class AddressGeocoded(BaseModel):
    address: str
    latitude: Latitude
    longitude: Longitude

def check_esqid_format(x:str):
    x = x.strip()
    if re.match(pattern="[A-Z0-9]{2}~[0-9]{5}", string=x):
        return x
    else:
        raise ValidationError(f"String '{x}' could not be interpreted as an esqID.")
EsqId = Annotated[str, AfterValidator(check_esqid_format)]