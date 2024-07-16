# File: libs/azure/functions/blueprints/smarty/activities/validate_addresses.py

from azure.durable_functions import Blueprint
from libs.utils.azure_storage import load_dataframe, export_dataframe
from libs.utils.smarty import bulk_validate, detect_column_names
import pandas as pd


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
            - 'columns_to_return' : A list indicating a slice of columns to return, including those from
                                the original data source and those generated by Smarty.

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
    df = load_dataframe(ingress["source"])

    # fill in gaps from the column_mapping variable using fuzzy matching (this is best-effort, not guaranteed to be accurate)
    mapping = detect_column_names(
        cols=df.columns, override_mappings=ingress.get("column_mapping", {})
    )

    # Perform bulk address validation
    validated = bulk_validate(
        df=df,
        address_col=mapping["street"],
        city_col=mapping["city"],
        state_col=mapping["state"],
        zip_col=mapping["zip"],
    )
    # slice return columns if a list was passed
    if ingress.get("columns_to_return"):
        validated = validated[ingress["columns_to_return"]]

    # Handle data output based on destination configuration
    if ingress.get("destination"):
        return export_dataframe(
            df=validated.where(pd.notna(validated), None).replace("Nan", None),
            destination=ingress["destination"],
        )
    else:
        # Return cleaned addresses as a dictionary
        return validated.to_dict(orient="records")
