BLUEPRINTS = {
    "esquire-auto-audience": [
        "libs/azure/functions/blueprints/azure/datalake/*",
        "libs/azure/functions/blueprints/azure/postgres/*",
        "libs/azure/functions/blueprints/azure/synapse/*",
        "libs/azure/functions/blueprints/esquire/audiences/builder/*",
        "libs/azure/functions/blueprints/esquire/audiences/egress/*",
        "libs/azure/functions/blueprints/esquire/audiences/utils/activities/*",
        "libs/azure/functions/blueprints/onspot/*",
        "libs/azure/functions/blueprints/purge_instance_history",
        "libs/azure/functions/blueprints/s3/*",
    ],
    "esquire-campaign-proposal": [
        "libs/azure/functions/blueprints/keep_alive",
        "libs/azure/functions/blueprints/azure/datalake/*",
        "libs/azure/functions/blueprints/esquire/reporting/campaign_proposal/*",
        "libs/azure/functions/blueprints/microsoft/graph/*",
        "libs/azure/functions/blueprints/purge_instance_history",
    ],
    "esquire-location-insights": [
        "libs/azure/functions/blueprints/keep_alive",
        "libs/azure/functions/blueprints/azure/datalake/*",
        "libs/azure/functions/blueprints/azure/synapse/*",
        "libs/azure/functions/blueprints/esquire/reporting/location_insights/*",
        "libs/azure/functions/blueprints/onspot/*",
        "libs/azure/functions/blueprints/microsoft/graph/*",
        "libs/azure/functions/blueprints/purge_instance_history",
    ],
    "esquire-dashboard-data": [
        "libs/azure/functions/blueprints/azure/datalake/*",
        "libs/azure/functions/blueprints/esquire/dashboard/*",
        "libs/azure/functions/blueprints/keep_alive",
        "libs/azure/functions/blueprints/meta/*",
        "libs/azure/functions/blueprints/oneview/reports/*",
        "libs/azure/functions/blueprints/onspot/*",
        "libs/azure/functions/blueprints/purge_instance_history",
        "libs/azure/functions/blueprints/azure/synapse/*",
    ],
    "esquire-docs": [
        "libs/azure/functions/blueprints/esquire/openapi/*",
    ],
    "esquire-oneview-tasks": [
        "libs/azure/functions/blueprints/oneview/tasks/*",
    ],
    "esquire-roku-sync": [
        "libs/azure/functions/blueprints/azure/datalake/*",
        "libs/azure/functions/blueprints/esquire/audiences/egress/oneview/*",
        "libs/azure/functions/blueprints/oneview/segments/*",
        "libs/azure/functions/blueprints/onspot/*",
        "libs/azure/functions/blueprints/purge_instance_history",
        "libs/azure/functions/blueprints/s3/*",
        "libs/azure/functions/blueprints/azure/synapse/*",
    ],
    "esquire-movers-sync": [
        "libs/azure/functions/blueprints/esquire/audiences/ingress/movers_sync/*",
        "libs/azure/functions/blueprints/azure/synapse/*",
        "libs/azure/functions/blueprints/purge_instance_history",
        "libs/azure/functions/blueprints/microsoft/graph/*",
    ],
    "esquire-sales-uploader": [
        "libs/azure/functions/blueprints/keep_alive",
        "libs/azure/functions/blueprints/esquire/reporting/matchback/salesUploader/*",
        "libs/azure/functions/blueprints/smarty/*",
    ],
    "esquire-google-leads": [
        "libs/azure/functions/blueprints/keep_alive",
        "libs/azure/functions/blueprints/esquire/google/leads_form/*",
        "libs/azure/functions/blueprints/microsoft/graph/*",
        "libs/azure/functions/blueprints/purge_instance_history",
    ],
    "esquire-unmasked": [
        "libs/azure/functions/blueprints/azure/datalake/activities/copy",
        "libs/azure/functions/blueprints/azure/datalake/activities/ftp",
        "libs/azure/functions/blueprints/aws/athena/*",
        "libs/azure/functions/blueprints/esquire/reporting/unmasked/*",
        "libs/azure/functions/blueprints/httpx",
        "libs/azure/functions/blueprints/microsoft/graph/*",
        "libs/azure/functions/blueprints/purge_instance_history",
        "libs/azure/functions/blueprints/smarty/*",
    ],
    "esquire-callback-reader": [
        "libs/azure/functions/blueprints/esquire/callback_reader"
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