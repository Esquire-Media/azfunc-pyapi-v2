# File: libs/azure/functions/blueprints/smarty/activities/validate_addresses.py

from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from fuzzywuzzy import fuzz
from libs.azure.functions import Blueprint
from libs.utils.smarty import bulk_validate, detect_column_names
import pandas as pd, os, logging


bp: Blueprint = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_smarty_validateAddresses(ingress: dict):
    """
    Validate and clean address data using the SmartyStreets API.

    Parameters
    ----------
    ingress : dict
        A dictionary containing configuration parameters and address data for validation.
        The dictionary can contain the following keys:
            - 'source': Source data for address validation. Can be a URL to a CSV file,
                        a dictionary specifying an Azure Blob storage location, or a list
                        of dictionaries containing address components.
            - 'column_mapping': A dictionary mapping the DataFrame columns to address components.
                                It is used to align input data to the expected format for
                                the SmartyStreets API. If this is missing or incomplete, fuzzy matching
                                will be used to make a best-effort guess as to the column mapping.
            - 'destination': Specifies the Azure Blob storage location for storing the cleaned
                              addresses. If not provided, the cleaned addresses will be returned.

    Returns
    -------
    dict or None
        If 'destination' is not provided in the ingress, returns a dictionary containing
        the cleaned addresses. If 'destination' is provided, the cleaned addresses are
        uploaded to the specified Azure Blob storage and None is returned.

    Raises
    ------
    ValueError
        If the source data format is not recognized or supported.

    Notes
    -----
    The 'source' key in the ingress dictionary is mandatory and can specify the address data
    in various formats. The 'column_mapping' and 'destination' keys are optional.
    """

    # Load DataFrame based on the source type specified in ingress
    if isinstance(ingress["source"], str):
        df = pd.read_csv(ingress["source"])
    elif isinstance(ingress["source"], dict):
        # Handling Azure Blob storage as the source
        blob = BlobClient.from_connection_string(
            conn_str=os.environ[ingress["source"]['conn_str']],
            container_name=ingress["source"]["container_name"],
            blob_name=ingress["source"]["blob_name"],
        )
        # Generate SAS token for blob access
        sas_token = generate_blob_sas(
            account_name=blob.account_name,
            account_key=blob.credential.account_key,
            container_name=blob.container_name,
            blob_name=blob.blob_name,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
        _, from_type = os.path.splitext(blob.blob_name)
        if not from_type:
            from_type = "csv"
        df = getattr(pd, "read_" + from_type.replace(".", ""))(
            blob.url + "?" + sas_token
        )
    elif isinstance(ingress["source"], list):
        # Handling list of dictionaries as the source
        df = pd.DataFrame(ingress["source"])
    else:
        raise ValueError("Unsupported source format.")


    # fill in gaps from the column_mapping variable using fuzzy matching (this is best-effort, not guaranteed to be accurate)
    mapping = detect_column_names(
        cols=df.columns,
        override_mappings=ingress.get('column_mapping',{})
    )

    # Perform bulk address validation
    validated = bulk_validate(
        df=df,
        address_col=mapping['street'],
        city_col=mapping['city'],
        state_col=mapping['state'],
        zip_col=mapping['zip'],
    )

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
            getattr(validated, "to_" + to_type.replace(".", ""))(),
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
        return validated.to_dict(orient="records")