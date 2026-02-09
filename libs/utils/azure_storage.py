from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient, BlobSasPermissions, generate_blob_sas
from azure.core.pipeline.transport import RequestsTransport
from functools import lru_cache
from urllib.parse import unquote
import datetime, os, pandas as pd, uuid, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@lru_cache(maxsize=1)
def _get_shared_session() -> requests.Session:
    """Create a session with optimized connection pooling for Azure Blob Storage."""
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "PUT", "POST", "DELETE", "OPTIONS"]
    )

    # Configure adapter with connection pool limits
    adapter = HTTPAdapter(
        pool_connections=10,       # Number of connection pools to cache
        pool_maxsize=20,           # Max connections per host
        max_retries=retry_strategy,
        pool_block=False
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


@lru_cache(maxsize=128)
def get_cached_blob_client(blob_url: str) -> BlobClient:
    """Returns a cached BlobClient with a shared transport for connection pooling.

    All calls return the same BlobClient instance for the same URL,
    which reuses the underlying HTTP transport and connection pool.
    """
    transport = RequestsTransport(
        session=_get_shared_session(),
        session_owner=False,  # Don't close our shared session
        connection_timeout=60,
        read_timeout=300
    )

    return BlobClient.from_blob_url(blob_url, transport=transport)


def download_blob_bytes(blob_url: str) -> bytes:
    """Download blob content using a cached client for connection reuse."""
    return get_cached_blob_client(blob_url).download_blob().readall()


def init_blob_client(**kwargs) -> BlobClient:
    """
    Create a BlobClient based on the provided keyword arguments.

    Parameters can include:
    - connection_string
    - account_url
    - container_name
    - blob_name
    - account_key
    - sas_token
    - blob_url

    Returns:
    - BlobClient object
    """
    transport = RequestsTransport(
        session=_get_shared_session(),
        session_owner=False,
        connection_timeout=60,
        read_timeout=300
    )

    if "blob_url" in kwargs:
        # Initialize using the blob URL
        return BlobClient.from_blob_url(kwargs["blob_url"], transport=transport)

    if "connection_string" in kwargs:
        # Initialize using the connection string
        return BlobClient.from_connection_string(
            kwargs["connection_string"], kwargs["container_name"], kwargs["blob_name"],
            transport=transport
        )

    if "conn_str" in kwargs:
        # Initialize using the connection string
        return BlobClient.from_connection_string(
            os.environ.get(kwargs["conn_str"], kwargs["conn_str"]),
            kwargs["container_name"],
            kwargs["blob_name"],
            transport=transport
        )

    if "account_url" in kwargs:
        if "sas_token" in kwargs:
            # Initialize using account URL and SAS token
            return BlobClient(
                account_url=kwargs["account_url"],
                container_name=kwargs["container_name"],
                blob_name=kwargs["blob_name"],
                credential=kwargs["sas_token"],
                transport=transport
            )
        if "account_key" in kwargs:
            # Initialize using account URL and account key
            return BlobClient(
                account_url=kwargs["account_url"],
                container_name=kwargs["container_name"],
                blob_name=kwargs["blob_name"],
                credential=kwargs["account_key"],
                transport=transport
            )

    raise ValueError(
        "Insufficient or incorrect parameters provided to initialize a BlobClient."
    )


def _create_transport():
    """Create a shared RequestsTransport instance for connection pooling."""
    return RequestsTransport(
        session=_get_shared_session(),
        session_owner=False,
        connection_timeout=60,
        read_timeout=300
    )


def get_container_client(connection_string: str, container_name: str) -> ContainerClient:
    """Create ContainerClient with shared transport for connection pooling.

    Args:
        connection_string: Azure Storage connection string
        container_name: Name of the container

    Returns:
        ContainerClient with shared transport
    """
    return ContainerClient.from_connection_string(
        connection_string,
        container_name=container_name,
        transport=_create_transport()
    )


def get_blob_service_client(connection_string: str = None) -> BlobServiceClient:
    """Create BlobServiceClient with shared transport for connection pooling.

    Args:
        connection_string: Optional Azure Storage connection string.
                          If None, uses default credential.

    Returns:
        BlobServiceClient with shared transport
    """
    if connection_string:
        return BlobServiceClient.from_connection_string(
            connection_string,
            transport=_create_transport()
        )
    # Use default credential when no connection string provided
    return BlobServiceClient(
        account_url=os.environ.get("AZURE_STORAGE_ACCOUNT_URL"),
        credential=os.environ.get("AZURE_STORAGE_KEY"),
        transport=_create_transport()
    )


def get_blob_sas(
    blob: BlobClient,
    expiry: datetime.timedelta = datetime.timedelta(days=2),
    prefix: str = "https://",
) -> str:
    """
    Given a BlobClient object and an expiry time, return a SAS url for that blob.
    """
    return (
        unquote(blob.url)
        + "?"
        + generate_blob_sas(
            account_name=blob.account_name,
            account_key=blob.credential.account_key,
            container_name=blob.container_name,
            blob_name=blob.blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.datetime.utcnow() + expiry,
        )
    ).replace("https://", prefix)


def query_entities_to_list_of_dicts(
    entities, partition_name: str = "PartitionKey", row_name: str = "RowKey"
):
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
            row_name: entity["RowKey"],
        }
        for key, value in entity.items():
            if key not in ["PartitionKey", "RowKey"]:
                converted_entity[key] = value
        result.append(converted_entity)

    return result


def load_dataframe(source: str | dict | list) -> pd.DataFrame:
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
            conn_str=os.environ[source["conn_str"]],
            container_name=source["container_name"],
            blob_name=source["blob_name"],
            transport=_create_transport()
        )
        # Generate SAS token for blob access
        sas_token = generate_blob_sas(
            account_name=blob.account_name,
            account_key=blob.credential.account_key,
            container_name=blob.container_name,
            blob_name=blob.blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.datetime.utcnow() + datetime.timedelta(days=2),
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


def export_dataframe(
    df: pd.DataFrame,
    destination: str | dict,
    expiry: datetime.timedelta = datetime.timedelta(days=2),
) -> str:
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
    - expiry (datetime.timedelta): Optional value specifying how long the returned URL will be valid. Default value of 2 days.

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
        blob = BlobClient.from_blob_url(destination, transport=_create_transport())
    # establish output blob from a blob details dictionary
    elif isinstance(destination, dict):
        to_type = destination.get("format", None)
        blob = BlobClient.from_connection_string(
            conn_str=os.environ[destination["conn_str"]],
            container_name=destination["container_name"],
            blob_name=destination.get(
                "blob_name",
                "{}/{}.{}".format(
                    destination["blob_prefix"],
                    uuid.uuid4().hex,
                    to_type if to_type else "csv",
                ),
            ),
            transport=_create_transport()
        )

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
    return get_blob_sas(blob, expiry)
