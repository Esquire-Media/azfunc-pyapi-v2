from pydantic import Field
from typing import Annotated

# Latitude and Longitude
Latitude = Annotated[float, Field(ge=-90), Field(le=90)]
Longitude = Annotated[float, Field(ge=-180), Field(le=180)]