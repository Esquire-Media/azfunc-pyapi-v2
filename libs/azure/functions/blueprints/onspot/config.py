from azure.data.tables import TableServiceClient
import os

# Make sure the required tables exist
existing_tables = list(
    (
        table_service := TableServiceClient.from_connection_string(
            conn_str=(
                ONSPOT_CONN_STR := os.environ.get(
                    "ONSPOT_CONN_STR", os.environ["AzureWebJobsStorage"]
                )
            )
        )
    ).list_tables()
)
table_name_prefix = (p := os.environ.get("ONSPOT_TABLE_PREFIX", "onspot")) + "Schedule"
for table_name in (
    ONSPOT_TABLE_NAMES := {
        "jobs": table_name_prefix + "Jobs",
        "runs": table_name_prefix + "Runs",
        "config": table_name_prefix + "Config",
    }
).values():
    if table_name not in existing_tables:
        try:
            table_service.create_table(table_name)
        except:
            pass
            

from libs.data import register_binding
import os

register_binding(
    "OnSpotTables",
    "KeyValue",
    "azure_table",
    client=table_service,
)
