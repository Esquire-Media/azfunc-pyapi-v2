from azure.data.tables import TableServiceClient, TableEntity
from logging import Handler
import os


class AzureTableHandler(Handler):
    def __init__(
        self,
        conn_str=os.environ.get("LOGGING_CONN_STR", os.environ["AzureWebJobsStorage"]),
        table_name=os.environ.get("LOGGING_TABLE_NAME", "logging"),
        *args,
        **kwargs,
    ):
        super(AzureTableHandler, self).__init__(*args, **kwargs)
        self._table_client = TableServiceClient.from_connection_string(
            conn_str=conn_str
        ).create_table_if_not_exists(table_name=table_name)

    def emit(self, record):
        entity: TableEntity = TableEntity(
            PartitionKey="log",
            RowKey=str(record.created),
            Message=record.getMessage(),
            Level=record.levelname,
        )
        if hasattr(record, "context"):
            for k,v in record.context.items():
                if v:
                    entity[k] = v
            
        self._table_client.create_entity(entity)
