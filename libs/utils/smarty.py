from smartystreets_python_sdk import StaticCredentials, ClientBuilder, Batch
from smartystreets_python_sdk.us_street import Lookup as StreetLookup
import pandas as pd
import os
from libs.azure.key_vault import KeyVaultClient
from fuzzywuzzy import fuzz

def get_items_recursive(obj, dict={}):
    """
    Iterates through all key-value pairs of an object and returns those pairs as a dictionary.
    If any pair's value is itself an object, recursively iterates through that object also.
    """
    # iterate through the dict items at this level
    for k, val in obj.__dict__.items():
        # if an item has its own dict, iterate through that recursively
        if hasattr(obj.__dict__[k], "__dict__"):
            get_items_recursive(obj.__dict__[k], dict)
        else:
            # for the items at this level, add them to the persistent list
            if not "__" in k:  # ignore a few metadata categories like '__doc__'
                dict[k] = val

    return dict


def bulk_validate(
    df:pd.DataFrame,
    address_col:str,
    addr2_col:str=None,
    city_col:str=None,
    state_col:str=None,
    zip_col:str=None,
) -> pd.DataFrame:
    """
    Accepts a dataframe containing address data in one or more component columns. Returns a dataframe with all returned Smarty columns.
    Smarty credentials will be read as environmental variables `SMARTY_APP_ID`, `SMARTY_APP_TOKEN`, and `SMARTY_LICENSE_ID`.
    If any of these variables are not set, credentials will be pulled from the `smarty-service` keyvault instead, in which case authorization is required to access the vault.

    * df : The dataframe containing addresses to clean

    * address_col : The name of the column containing address data (If only this column is set, freeform address will be assumed)
    * city_col : The name of the column containing city data
    * state_col : The name of the column containing state data
    * zip_col : The name of the column containing zipcode data

    ---
    match codes glossary:
        * Y - Confirmed in USPS data.
        * N - Not confirmed in USPS data.
        * S - Confirmed by ignoring secondary info.
        * D - Confirmed but missing secondary info.
        * None - Not present in USPS database.

    For more info on match codes: https://www.smarty.com/docs/cloud/us-street-api#dpvmatchcode
    """

    if not len(df):
        raise IndexError("Empty Dataframe passed to the bulk_validate function")

    # use environmental variables if all exist
    if all([os.environ.get("SMARTY_APP_ID"), os.environ.get("SMARTY_APP_TOKEN"), os.environ.get("SMARTY_LICENSE_ID"),]):
        smarty_id = os.environ.get("SMARTY_APP_ID")
        smarty_token = os.environ.get("SMARTY_APP_TOKEN")
        smarty_license = os.environ.get("SMARTY_LICENSE_ID")
    # if no env are set, connect to the keyvault to load auth variables instead
    else:
        client = KeyVaultClient("smarty-service")
        smarty_id = client.get_secret("smarty-id").value
        smarty_token = client.get_secret("smarty-token").value
        smarty_license = client.get_secret("smarty-license").value

    # reset index (because we merge on this later)
    df = df.reset_index(drop=True)

    # convert all address data to strings
    for col in address_col, city_col, state_col, zip_col:
        if col != None:
            df[col] = df[col].astype(str)

    # authentication
    credentials = StaticCredentials(smarty_id, smarty_token)
    # launch the street lookup client
    client = (
        ClientBuilder(credentials)
        .with_licenses([smarty_license])
        .build_us_street_api_client()
    )

    # initialize the first batch and the list to store results
    batch = Batch()
    data_list = []

    # build batches and send lookups
    for i, row in df.reset_index(drop=True).iterrows():
        lookup = StreetLookup()

        # add data for the address field
        lookup.street = row[address_col]

        # check if we have an address2
        if addr2_col != None:
            val = row.get(addr2_col)
            if val and len(val.strip()) > 0:
                lookup.street2 = row[addr2_col]

        # check that city field is specified and data is not empty, then set data
        if city_col != None:
            if len(row[city_col]) > 0:
                lookup.city = row[city_col]
        # check that state field is specified and data is not empty, then set data
        if state_col != None:
            if len(row[state_col]) > 0:
                lookup.state = row[state_col]
        # check that zipcode field is specified and data is not empty, then set data
        if zip_col != None:
            if len(row[zip_col]) > 0:
                lookup.zipcode = row[zip_col]

        # set lookup settings and add to batch
        lookup.candidates = 1  # return only the best candidate
        lookup.match = (
            "invalid"  # include best match even if not a valid mailable address
        )
        batch.add(lookup)

        # send batch once it hits the max batch size (100) or the last row
        if batch.is_full() or i == len(df) - 1:
            # send the batch
            client.send_batch(batch)

            for i, b in enumerate(batch):
                candidates = b.result
                if len(candidates) > 0:
                    best = candidates[0]
                    # recursively get the info at all levels of the cleaned data (there is a multi-level dict hierarchy containing the data)
                    # Important to make this a copy, otherwise all dicts will end up as pointers to the last item in the batch
                    info_dict = get_items_recursive(best)
                    data_list.append(info_dict.copy())
                else:
                    data_list.append(
                        {
                            "dpv_match_code": None,
                        }
                    )

            # restart an empty Batch
            batch = Batch()

    # prevent duplicate columns in the output by dropping the original column for any name conflicts
    dupe_cols = [col for col in list(data_list[0].keys()) if col in df.columns]
    original = df.drop(columns=[address_col] + dupe_cols)

    # return the cleaned data merged with any non-duplicated columns from the original data
    cleaned = pd.merge(
        original, pd.DataFrame(data_list), right_index=True, left_index=True
    )

    return cleaned

def detect_column_names(cols:list, override_mappings:dict={}) -> dict:
    """
    Detects and maps column names from a list to predefined address components, optionally applying custom mappings.

    This function attempts to match each provided column name to a set of common address components
    ('street', 'city', 'state', 'zip') using fuzzy matching. The user can override default mappings by
    providing a `column_mappings` dictionary. If the `inverted` flag is set to True, the function
    inverts the resulting mapping, swapping keys with their corresponding values.

    Parameters:
    - cols (list): A list of column names from which to detect address components.
    - override_mappings (dict, optional): A dictionary where keys are address components and values are
      the column names to be explicitly mapped to these components. Defaults to an empty dictionary.

    Returns:
    - dict: A dictionary mapping address components to detected column names. If `inverted` is True,
      the dictionary maps column names to address components instead.

    Note:
    The function uses fuzzy logic to determine the best match for each address component. This may not
    always result in perfect matches, especially with unconventional or highly abbreviated column names.
    Providing explicit `override_mappings` can help ensure accuracy.
    """
    # dictionary of common column headers for address components
    # this will be modified in-place to build the output dictionary
    mapping = {
        "street": ["address", "street", "delivery_line_1"],
        "city": ["city"],
        "state": ["state"],
        "zip": ["zip", "zipcode", "postal", "postalcode"],
    }

    # find best fit for each address field
    for key, defaults in mapping.items():
        # if key is in the column mappings dictionary, use that default value
        if key in override_mappings and override_mappings[key] in cols:
            mapping[key] = override_mappings[key]
        # otherwise, use fuzzy matching to find the best match
        else:
            column_scores = [
                max([fuzz.WRatio(column.upper(), default.upper()) for default in defaults])
                for column in cols
            ]
            if key == 'state':
                print([*zip(cols, column_scores)])
            best_fit_idx = column_scores.index(max(column_scores))
            best_fit = cols[best_fit_idx]
            mapping[key] = best_fit

    return mapping