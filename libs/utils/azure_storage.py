from azure.storage.blob import(
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas
)
from datetime import datetime as dt, timedelta

def query_entities_to_list_of_dicts(entities, partition_name:str='PartitionKey', row_name:str='RowKey'):
    """
    Convert the results of an Azure storage table query into a list of dictionaries with keys renamed.

    Params:
    partition_name : string to use in renaming the partitionkey data
    row_name       : string to use in renaming the rowkey data
    """

    result = []
    for entity in entities:
        converted_entity = {
            partition_name: entity["PartitionKey"],
            row_name: entity["RowKey"]
        }
        for key, value in entity.items():
            if key not in ["PartitionKey", "RowKey"]:
                converted_entity[key] = value
        result.append(converted_entity)

    return result

def get_blob_sas(blob:BlobClient, expiry:timedelta=timedelta(days=2), prefix:str="https://") -> str:
    """
    Given a BlobClient object and an expiry time, return a SAS url for that blob.
    """
    return (
        blob.url
        + "?"
        + generate_blob_sas(
            account_name=blob.account_name,
            account_key=blob.credential.account_key,
            container_name=blob.container_name,
            blob_name=blob.blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=dt.utcnow() + timedelta(days=2),
        )
    ).replace("https://", prefix)
