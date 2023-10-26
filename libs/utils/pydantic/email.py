from pydantic import AfterValidator, ValidationError
from typing import Annotated
import re

# Email Address
def check_email_format(x:str):
    x = x.strip()
    if re.match(pattern="[^@]+@[^@]+\.[^@]+", string=x):
        return x
    else:
        raise ValidationError(f"String '{x}' could not be interpreted as an email address.")
EmailAddress = Annotated[str, AfterValidator(check_email_format)]