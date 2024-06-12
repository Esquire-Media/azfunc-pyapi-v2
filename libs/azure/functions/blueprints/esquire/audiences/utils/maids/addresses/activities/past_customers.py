# File: libs/azure/functions/blueprints/esquire/audiences/maids/addresses/activities/new_movers.py

from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from azure.durable_functions import Blueprint
from typing import AnyStr, Dict, Union
import pandas as pd, os

bp = Blueprint()

# activity to fill in the geo data for each audience object
@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesMaidsAddresses_pastCustomers(ingress: dict):
    """
    Activity function to process digital neighbor addresses.

    This function handles fetching address data from Azure Blob Storage,
    processes it, and then either uploads the processed data back to Blob Storage
    or returns it as a dictionary based on the provided destination configuration.

    Parameters
    ----------
    ingress : dict
        A dictionary containing the following keys:
        - source: str or dict
            The source from which to fetch the address data. Can be a URL string or a
            dictionary specifying Blob Storage details (including 'conn_str', 'container_name',
            and 'blob_name').
        - destination: str or dict, optional
            The destination for storing the processed data. Can be a URL string or a
            dictionary specifying Blob Storage details (including 'conn_str', 'container_name',
            'blob_name', and optionally 'format' to specify the output file format).

    Returns
    -------
    Union[str, Dict]
        If a destination is provided, returns the SAS URL of the uploaded blob.
        Otherwise, returns the processed addresses as a dictionary.
    """

    addresses = get_addresses(source=ingress["source"])

    # Handle data output based on destination configuration
    if ingress.get("destination"):
        # Configuring BlobClient for data upload
        if isinstance(ingress["destination"], str):
            blob = BlobClient.from_blob_url(ingress["destination"])
        elif isinstance(ingress["destination"], dict):
            blob = BlobClient.from_connection_string(
                conn_str=os.environ[ingress["destination"]["conn_str"]],
                container_name=ingress["destination"]["container_name"],
                blob_name=ingress["destination"]["blob_name"],
            )
            to_type = ingress["destination"].get("format", None)
        if not to_type:
            _, to_type = os.path.splitext(blob.blob_name)
            if not to_type:
                to_type = "csv"
        blob.upload_blob(
            getattr(addresses, "to_" + to_type.replace(".", ""))(),
            overwrite=True,
        )

        return (
            blob.url
            + "?"
            + generate_blob_sas(
                account_name=blob.account_name,
                account_key=blob.credential.account_key,
                container_name=blob.container_name,
                blob_name=blob.blob_name,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + relativedelta(days=2),
            )
        )
    else:
        # Return cleaned addresses as a dictionary
        return addresses.to_dict(orient="records")


def get_addresses(source: Union[AnyStr, Dict]):
    """
    Fetches address data from a source (URL or dictionary specifying Blob Storage details).

    Parameters
    ----------
    source : Union[AnyStr, Dict]
        The source from which to fetch the data. Can be a URL string or a dictionary
        with Blob Storage connection details.

    Returns
    -------
    DataFrame
        The addresses data as a pandas DataFrame.
    """
    
    if isinstance(source, str):
        blob = BlobClient.from_blob_url(source)
    elif isinstance(source, dict):
        blob = BlobClient.from_connection_string(
            conn_str=os.environ[source["conn_str"]],
            container_name=source["container_name"],
            blob_name=source["blob_name"],
        )
        from_type = source.get("format", None)
    if not from_type:
        _, from_type = os.path.splitext(blob.blob_name)
        if not from_type:
            from_type = "csv"
    df = getattr(pd, "read_" + from_type.replace(".", ""))(
        blob.url
        + "?"
        + generate_blob_sas(
            account_name=blob.account_name,
            account_key=blob.credential.account_key,
            container_name=blob.container_name,
            blob_name=blob.blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    )
    return df