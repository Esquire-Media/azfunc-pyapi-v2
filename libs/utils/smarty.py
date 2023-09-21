from smartystreets_python_sdk import StaticCredentials, ClientBuilder, Batch
from smartystreets_python_sdk.us_street import Lookup as StreetLookup
import pandas as pd
import os

def get_items_recursive(obj, dict={}):
    """
    Iterates through all key-value pairs of an object and returns those pairs as a dictionary. 
    If any pair's value is itself an object, recursively iterates through that object also.
    """
    # iterate through the dict items at this level
    for k, val in obj.__dict__.items():
        # if an item has its own dict, iterate through that recursively
        if hasattr(obj.__dict__[k], '__dict__'):
            get_items_recursive(obj.__dict__[k], dict)
        else:

            # for the items at this level, add them to the persistent list
            if not '__' in k:  # ignore a few metadata categories like '__doc__'
                dict[k] = val
        
    return dict

def bulk_validate(df, address_col, city_col=None, state_col=None, zip_col=None, auth_id=os.environ['ss_id'], auth_token=os.environ['ss_token']) -> pd.DataFrame:
    """
    Accepts a dataframe containing address data in one or more component columns. Returns a dataframe with all returned Smarty columns.

    * df : The dataframe containing addresses to clean
    * auth_id : Smarty authentication id
    * auth_token : Smarty authentication token

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

    # reset index (because we merge on this later)
    df = df.reset_index(drop=True)
    
    # convert all address data to strings
    for col in address_col, city_col, state_col, zip_col:
        if col != None:
            df[col] = df[col].astype(str)

    # authentication
    credentials = StaticCredentials(auth_id, auth_token)
    # launch the street lookup client
    client = ClientBuilder(credentials).with_licenses(["us-core-enterprise-cloud"]).build_us_street_api_client()

    # initialize the first batch and the list to store results
    batch = Batch()
    data_list = []

    # build batches and send lookups
    for i, row in df.reset_index(drop=True).iterrows():
        lookup = StreetLookup()

        # add data for the address field
        lookup.street = row[address_col]

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
        lookup.candidates=1 # return only the best candidate
        lookup.match='invalid' # include best match even if not a valid mailable address
        batch.add(lookup)

        # send batch once it hits the max batch size (100) or the last row
        if batch.is_full() or i == len(df)-1:
           
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
                    data_list.append({
                        'dpv_match_code':None,
                    })

            # restart an empty Batch
            batch = Batch()
    
    # prevent duplicate columns in the output by dropping the original column for any name conflicts
    dupe_cols = [col for col in list(data_list[0].keys()) if col in df.columns]
    original = df.drop(columns=[address_col] + dupe_cols)

    # return the cleaned data merged with any non-duplicated columns from the original data
    cleaned = pd.merge(original, pd.DataFrame(data_list), right_index=True, left_index=True)

    return cleaned