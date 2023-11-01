from azure.data.tables import TableClient
from time import sleep

def recreate(table: TableClient):
    table.delete_table()
    ready = False
    while not ready:
        try:
            # Try to recreate the table
            table.create_table()
            ready = True
        except:
            sleep(5)

