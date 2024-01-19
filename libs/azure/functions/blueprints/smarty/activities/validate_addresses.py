# File: libs/azure/functions/blueprints/smarty/activities/validate_addresses.py

from azure.storage.blob import BlobClient, BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from fuzzywuzzy import fuzz
from libs.azure.functions import Blueprint
from libs.utils.smarty import bulk_validate
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
                                the SmartyStreets API.
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

    # Detecting column names for address components
    mapped = detect_column_names(df)
    # Perform bulk address validation
    validated = bulk_validate(
        df=mapped,
        address_col=ingress.get("column_mapping", {}).get(
            "address", "street" if "street" in mapped.columns else None
        ),
        city_col=ingress.get("column_mapping", {}).get(
            "city", "city" if "city" in mapped.columns else None
        ),
        state_col=ingress.get("column_mapping", {}).get(
            "state", "state" if "state" in mapped.columns else None
        ),
        zip_col=ingress.get("column_mapping", {}).get(
            "zip", "zip" if "zip" in mapped.columns else None
        ),
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


def detect_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect and rename address component columns in a DataFrame.

    This function attempts to automatically detect the columns corresponding to
    address components in a DataFrame based on common naming patterns. It then renames
    these columns to a standardized format for further processing.

    Parameters
    ----------
    df : pd.DataFrame
        A DataFrame containing address data with potentially varied column naming.

    Returns
    -------
    pd.DataFrame
        A modified DataFrame with renamed columns for standardized address components:
        'street', 'city', 'state', and 'zip'.

    Notes
    -----
    The function uses fuzzy string matching to identify the best matches for each address
    component based on a predefined list of common column names. It returns a DataFrame
    slice with only the detected and renamed address component columns.
    """

    # dictionary of common column headers for address components
    mapping = {
        "street": ["address", "street", "delivery_line_1", "line1", "add"],
        "city": ["city", "city_name"],
        "state": ["state", "st", "state_abbreviation"],
        "zip": ["zip", "zipcode", "postal", "postalcodeid"],
    }

    # find best fit for each address field
    for dropdown, defaults in mapping.items():
        column_scores = [
            max([fuzz.ratio(column.upper(), default.upper()) for default in defaults])
            for column in df.columns
        ]
        best_fit_idx = column_scores.index(max(column_scores))
        best_fit = df.columns[best_fit_idx]
        df = df.rename(columns={best_fit: dropdown})

    return df[["street", "city", "state", "zip"]]
