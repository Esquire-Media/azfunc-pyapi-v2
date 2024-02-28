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

class AddressComponents2(BaseModel):
    street: str
    city: str
    state: str
    zipcode: str

class AddressGeocoded(BaseModel):
    address: str
    latitude: Latitude
    longitude: Longitude

# Placekey
def check_placekey_format(x:str):
    x = x.strip()
    if re.match(pattern="^[a-zA-Z0-9\-]+@[a-zA-Z0-9\-]+$", string=x):
        return x
    else:
        raise ValidationError(f"String '{x}' could not be interpreted as a placekey.")
Placekey = Annotated[str, AfterValidator(check_placekey_format)]

def check_esqid_format(x:str):
    x = x.strip()
    if re.match(pattern="[A-Z0-9]{2}~[0-9]{5}", string=x):
        return x
    else:
        raise ValidationError(f"String '{x}' could not be interpreted as an esqID.")
EsqId = Annotated[str, AfterValidator(check_esqid_format)]