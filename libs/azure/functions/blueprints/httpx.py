from azure.durable_functions import Blueprint
import httpx

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

# Define an activity function
@bp.activity_trigger(input_name="ingress")
def activity_httpx(ingress: dict):
    """
    Params:
        -method (str)
        -url (str)
        -data (str)
        -headers (dict)

    Ex.    
    {
        "method":"POST",
        "url":"https://google.com",
        "data":"myData,
        "headers":{
            "Content-Type":"application/json"
        }
    }
    """

    response = httpx.request(
        **ingress,
        timeout=None
    )

    return {
        "headers":dict(response.headers),
        "body":str(response.content),
        "status_code":response.status_code,
    }