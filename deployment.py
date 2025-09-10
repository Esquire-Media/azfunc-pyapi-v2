BLUEPRINTS = {
    "esquire-auto-audience": [
        "libs/azure/functions/blueprints/azure/datalake/*",
        "libs/azure/functions/blueprints/azure/postgres/*",
        "libs/azure/functions/blueprints/azure/synapse/*",
        "libs/azure/functions/blueprints/esquire/audiences/builder/*",
        "libs/azure/functions/blueprints/esquire/audiences/egress/*",
        "libs/azure/functions/blueprints/esquire/audiences/utils/activities/*",
        "libs/azure/functions/blueprints/microsoft/graph/*",
        "libs/azure/functions/blueprints/meta/*",
        "libs/azure/functions/blueprints/onspot/*",
        "libs/azure/functions/blueprints/purge_instance_history",
        "libs/azure/functions/blueprints/s3/*",
    ],
    "esquire-autopolygon": [
        "libs/azure/functions/blueprints/esquire/rooftop_polys/*",
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
        "libs/azure/functions/blueprints/esquire/dashboard/onspot/*",
        "libs/azure/functions/blueprints/keep_alive",
        "libs/azure/functions/blueprints/meta/endpoints/creative_preview",
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
    "esquire-redshift-sync": [
        "libs/azure/functions/blueprints/aws/redshift_sync",
    ],
    "esquire-sales-uploader": [
        "libs/azure/functions/blueprints/keep_alive",
        "libs/azure/functions/blueprints/esquire/reporting/matchback/salesUploader/*",
        "libs/azure/functions/blueprints/smarty/*",
    ],
    "esquire-callback-reader": [
        "libs/azure/functions/blueprints/esquire/callback_reader"
    ],
    "debug": [
        "libs/azure/functions/blueprints/keep_alive",
        "libs/azure/functions/blueprints/logger",
    ],
    "esquire-sales-ingestion":[
        "libs/azure/functions/blueprints/keep_alive",
        "libs/azure/functions/blueprints/esquire/sales_ingestor/*",
        "libs/azure/functions/blueprints/microsoft/graph/*",
        "libs/azure/functions/blueprints/purge_instance_history",
    ],
    # !!! DANGER ZONE !!!
    "debug_env": [
        # !!! CAUTION !!!
        "libs/azure/functions/blueprints/env",  # DO NOT EVER ENABLE THIS IN ANY PUBLIC ENVIRONMENT
        # !!! WARNING !!!
    ],
    # !!! SECRET DATA !!!
}