import os
try:
    from development import BLUEPRINTS as DEV_BPS
except:
    DEV_BPS = []

BLUEPRINTS = {
    "esquire-docs": [
        "libs/azure/functions/blueprints/docs/*",
    ],
    "esquire-oneview-tasks": [
        "libs/azure/functions/blueprints/oneview/tasks/*",
    ],
    "esquire-dashboard-data": [
        "libs/azure/functions/blueprints/datalake/*",
        "libs/azure/functions/blueprints/meta/*",
        "libs/azure/functions/blueprints/onspot/*",
        "libs/azure/functions/blueprints/synapse/*",
        "libs/azure/functions/blueprints/esquire/dashboard/*",
    ],
    "debug": [
        "libs/azure/functions/blueprints/keep_alive",
        "libs/azure/functions/blueprints/logger",
    ],
    "debug_env": [
        "libs/azure/functions/blueprints/env",  # DO NOT ENABLE THIS IN ANY PUBLIC ENVIRONMENT
    ],
}


def get_bps(debug=False) -> list:
    return (
        BLUEPRINTS.get(os.environ.get("WEBSITE_SITE_NAME", ""), [])
        + (BLUEPRINTS["debug"] if debug else [])
        + (
            BLUEPRINTS["debug_env"]
            if debug and not os.environ.get("WEBSITE_SITE_NAME")
            else []
        ) + DEV_BPS
    )
