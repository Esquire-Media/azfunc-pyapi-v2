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
        "libs/azure/functions/blueprints/oneview/*",
        "libs/azure/functions/blueprints/onspot/*",
        "libs/azure/functions/blueprints/synapse/*",
        "libs/azure/functions/blueprints/esquire/dashboard/*",
    ],
    "esquire-roku-sync": [
        "libs/azure/functions/blueprints/datalake/*",
        "libs/azure/functions/blueprints/esquire/audiences/oneview/*",
        "libs/azure/functions/blueprints/oneview/segments/*",
        "libs/azure/functions/blueprints/onspot/*",
        "libs/azure/functions/blueprints/purge_instance_history",
        "libs/azure/functions/blueprints/s3/*",
        "libs/azure/functions/blueprints/synapse/*",
    ],
    "debug": [
        "libs/azure/functions/blueprints/keep_alive",
        "libs/azure/functions/blueprints/logger",
    ],
    # !!! DANGER ZONE !!!
    "debug_env": [
        # !!! CAUTION !!!
        "libs/azure/functions/blueprints/env",  # DO NOT EVER ENABLE THIS IN ANY PUBLIC ENVIRONMENT
        # !!! WARNING !!!
    ],
    # !!! SECRET DATA !!!
}


def get_bps(debug=False) -> list:
    return (
        BLUEPRINTS.get(os.environ.get("WEBSITE_SITE_NAME", ""), [])
        + (BLUEPRINTS["debug"] if debug else [])
        + (
            BLUEPRINTS["debug_env"]
            if debug and not os.environ.get("WEBSITE_SITE_NAME")
            else []
        )
        + DEV_BPS
    )
