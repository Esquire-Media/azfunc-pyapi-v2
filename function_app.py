from azure.functions import AuthLevel
from libs.azure.functions import FunctionApp

app = FunctionApp(http_auth_level=AuthLevel.ANONYMOUS)

app.register_blueprints(
    [
        # "libs/azure/functions/blueprints/keepalive",
        # "libs/azure/functions/blueprints/logger",
        # "libs/azure/functions/blueprints/daily_audience_generation/*",
        # "libs/azure/functions/blueprints/oneview/*",
        
        "libs/azure/functions/blueprints/datalake/*",
        "libs/azure/functions/blueprints/synapse/*",
        "libs/azure/functions/blueprints/onspot/*",
        "libs/azure/functions/blueprints/esquire/dashboard/onspot/*",
    ]
)
