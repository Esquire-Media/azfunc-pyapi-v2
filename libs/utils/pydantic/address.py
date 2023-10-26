from libs.utils.pydantic.geometry import Latitude, Longitude
from pydantic import BaseModel

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