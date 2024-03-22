from azure.storage.blob import(
    BlobClient,
    BlobSasPermissions,
    generate_blob_sas
)
from datetime import datetime as dt, timedelta
import pandas as pd
import os
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


def load_dataframe(source:str|dict|list) -> pd.DataFrame:
    """
    Loads a DataFrame from a specified source which can be a file path, a dictionary
    representing Azure Blob storage details, or a list of dictionaries representing row data.

    Parameters:
    - source (str | dict | list): The source from which to load the DataFrame. This can be:
        - A string specifying the blob SAS URI if it points to a CSV file. The function will
          read the CSV file into a DataFrame.
        - A dictionary with keys 'conn_str', 'container_name', and 'blob_name' specifying the
          Azure Blob storage details if the source is an Azure Blob. The function will read the
          blob content into a DataFrame, supporting dynamic file format recognition and
          generation of SAS tokens for blob access.
        - A list of dictionaries if the source is raw data. The function will convert this list
          into a DataFrame.

    Returns:
    - pd.DataFrame: The loaded DataFrame.

    Raises:
    - ValueError: If the source format is not supported (i.e., not a string, dictionary, or list).

    Note:
    - The Azure Blob storage option requires appropriate permissions and environment variables
      setup for `conn_str`. It supports dynamic reading of different file formats based on the
      blob file's extension, defaulting to CSV if no extension is found.
    - This function requires pandas, os, and from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions
      libraries, and assumes the environment is correctly configured for their use.
    """
    # Load DataFrame based on the source type specified in ingress
    if isinstance(source, str):
        return pd.read_csv(source)
    elif isinstance(source, dict):
        # Handling Azure Blob storage as the source
        blob = BlobClient.from_connection_string(
            conn_str=os.environ[source['conn_str']],
            container_name=source["container_name"],
            blob_name=source["blob_name"],
        )
        # Generate SAS token for blob access
        sas_token = generate_blob_sas(
            account_name=blob.account_name,
            account_key=blob.credential.account_key,
            container_name=blob.container_name,
            blob_name=blob.blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=dt.utcnow() + timedelta(days=2),
        )
        _, from_type = os.path.splitext(blob.blob_name)
        if not from_type:
            from_type = "csv"
        return getattr(pd, "read_" + from_type.replace(".", ""))(
            blob.url + "?" + sas_token
        )
    elif isinstance(source, list):
        # Handling list of dictionaries as the source
        return pd.DataFrame(source)
    else:
        raise ValueError("Unsupported source format.")
    
def export_dataframe(df:pd.DataFrame, destination:str|dict, expiry:timedelta=timedelta(days=2)) -> str:
    """
    Exports a DataFrame to a specified destination. The destination can be a blob URL or
    a dictionary specifying Azure Blob storage details.

    Parameters:
    - df (pd.DataFrame): The DataFrame to be exported.
    - destination (str | dict): The destination for exporting the DataFrame. This can be:
        - A string specifying a blob URL. The function will directly upload the DataFrame
          to this blob URL.
        - A dictionary with keys 'conn_str', 'container_name', and 'blob_name' (optionally 'format' to indicate the file type)
          specifying the Azure Blob storage details. The function will export the DataFrame to
          this specified blob in Azure Blob storage.
        - A list indicating multiple destinations is not directly supported in this version of the function.
    - expiry (timedelta): Optional value specifying how long the returned URL will be valid. Default value of 2 days.

    Returns:
    - str: The URL of the exported blob including the generated SAS token for read access.

    Raises:
    - ValueError: If the destination format is not supported.

    Note:
    - For Azure Blob storage destinations, the function dynamically identifies the file format
      to export based on the 'format' key in the destination dictionary or the file extension
      in 'blob_name'. It defaults to CSV if no format is provided or identified.
    - The SAS token generated for the blob URL grants read access for 2 days from the time of
      generation.
    """
    
    # establish output blob from a destination url
    if isinstance(destination, str):
        blob = BlobClient.from_blob_url(destination)
    # establish output blob from a blob details dictionary
    elif isinstance(destination, dict):
        blob = BlobClient.from_connection_string(
            conn_str=os.environ[destination["conn_str"]],
            container_name=destination["container_name"],
            blob_name=destination["blob_name"],
        )
        to_type = destination.get("format", None)
    
    # attempt to infer file type from the blob name
    if not to_type:
        _, to_type = os.path.splitext(blob.blob_name)
        if not to_type:
            to_type = "csv"
    # Determine the correct format and whether to include index in the output
    data_format = to_type.replace(".", "")
    if data_format == "csv":
        # Convert DataFrame to CSV without index if the format is CSV
        csv_data = df.to_csv(index=False)
        blob.upload_blob(csv_data, overwrite=True)
    else:
        # For other formats, use the appropriate pandas DataFrame method
        # If new formats are supported, they should be handled here
        data = getattr(df, "to_" + data_format)()
        blob.upload_blob(data, overwrite=True)

    # return a sas url for the exported blob
    return (
        blob.url
        + "?"
        + generate_blob_sas(
            account_name=blob.account_name,
            account_key=blob.credential.account_key,
            container_name=blob.container_name,
            blob_name=blob.blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=dt.utcnow() + expiry,
        )
    )