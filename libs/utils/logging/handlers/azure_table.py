from azure.data.tables import TableServiceClient, TableEntity
import logging
import os


class AzureTableHandler(logging.Handler):
    def __init__(
        self,
        table_name: str = os.environ.get("LOGGING_TABLE_NAME", "logging"),
        connection_string: str = os.environ.get(
            "LOGGING_CONN_STR", os.environ["AzureWebJobsStorage"]
        ),
    ):
        super().__init__()
        self.table_service: TableServiceClient = (
            TableServiceClient.from_connection_string(connection_string)
        )
        self.table_name: str = table_name

        # Create the table if it doesn't exist
        self.table_service.create_table_if_not_exists(self.table_name)

    def emit(self, record: logging.LogRecord):
        log_entry: str = self.format(record)
        entity: TableEntity = TableEntity(
            PartitionKey="log",
            RowKey=str(record.created),
            Message=log_entry,
            Level=record.levelname,
        )

        # Add invocation_id if it's included in the log record
        if hasattr(record, "invocation_id"):
            entity["InvocationId"] = record.invocation_id

        self.table_service.get_table_client(self.table_name).create_entity(
            entity=entity
        )
