# File: libs/azure/functions/suborchestrators.py

from azure.data.tables import TableClient


def get_sub_orchestrator_ids(table: TableClient, parent_instance_id: str):
    """
    Retrieve sub-orchestrator IDs associated with a parent orchestrator instance.

    Parameters
    ----------
    table : TableClient
        Azure Table Client instance to interact with Azure Table Storage.
    parent_instance_id : str
        ID of the parent orchestrator instance.

    Returns
    -------
    list
        List of sub-orchestrator IDs.
    """
    entities = list(
        table.query_entities(
            f"PartitionKey eq '{parent_instance_id}'", select=["ExecutionId"]
        )
    )

    if not entities:
        return []

    ids = []
    for e in entities:
        for i in table.query_entities(
            "PartitionKey ge '{}' and PartitionKey le '{}'".format(
                e["ExecutionId"] + ":",
                e["ExecutionId"] + chr(ord(":") + 1),
            ),
            select=["PartitionKey"],
        ):
            ids.append(i["PartitionKey"])
            ids.extend(get_sub_orchestrator_ids(table, i["PartitionKey"]))

    return ids
