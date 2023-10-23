from pydantic import BaseModel, Field, conlist, AfterValidator, ValidationError
from typing import Annotated, List, Union
import re
import pandas as pd
from azure.data.tables import TableServiceClient

# Latitude and Longitude
Latitude = Annotated[float, Field(ge=-90), Field(le=90)]
Longitude = Annotated[float, Field(ge=-180), Field(le=180)]

# Email Address
def check_email_format(x:str):
    x = x.strip()
    if re.match(pattern="[^@]+@[^@]+\.[^@]+", string=x):
        return x
    else:
        raise ValidationError(f"String '{x}' could not be interpreted as an email address.")
EmailAddress = Annotated[str, AfterValidator(check_email_format)]