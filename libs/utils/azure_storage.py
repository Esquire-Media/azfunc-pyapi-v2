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