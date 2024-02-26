from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient
import os
import json
import jwt
import pandas as pd
from azure.data.tables import TableClient
import hashlib
import logging
from pydantic import BaseModel, conlist
from typing import Optional
from libs.utils.logging import AzureTableHandler
from pydantic import validator
from libs.utils.oauth2.tokens.microsoft import ValidateMicrosoft
from libs.utils.oauth2.tokens import TokenValidationError
from libs.utils.pydantic.address import AddressComponents2
from datetime import datetime as dt, timedelta
import requests

bp = Blueprint()

freshness_window = timedelta(days=365)

# initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("placekey.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)


@bp.route(route="esquire/placekeys/fromAddress", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_placekeys_fromAddress(req: HttpRequest, client: DurableOrchestrationClient):
    """
    Endpoint for converting an address to a placekey, and then caching the results in a storage table.
    The ouput will return a numerical index corresponding to the order of the input addresses.

    Input:
    {
        "addresses":[
            {
                "street":"6013 MERIDIAN DR",
                "city":"BRYANT",
                "state":"AR",
                "zipcode":"72022"
            },
            ...
        ]
    }

    Output:
    {
        [
            {
                "index":0,
                "placekey":"0c2hlvuz5k@8f2-55m-nt9"
            },
            ...
        ]
    }
    """
    logger = logging.getLogger("placekey.logger")

    # load the request payload as a Pydantic object
    payload = HttpRequest.pydantize_body(req, PlacekeyPayload).model_dump()

    # # validate the MS bearer token to ensure the user is authorized to make requests
    # try:
    #     validator = ValidateMicrosoft(
    #         tenant_id=os.environ['MS_TENANT_ID'], 
    #         client_id=os.environ['MS_CLIENT_ID']
    #     )
    #     headers = validator(req.headers.get('authorization'))
    # except TokenValidationError as e:
    #     return HttpResponse(status_code=401, body=f"TokenValidationError: {e}")
    
    # # extract user information from bearer token metadata
    # payload['user'] = headers['oid']

    logging.warning(payload)

    # ingest payload addresses and create an md5 hash of the address
    df = pd.DataFrame(payload['addresses'], dtype=str)

    # create an index column for organizing response data (or use a passed one)
    if 'index' not in df.columns:
        df = df.reset_index()

    # generate an md5 hash for each address, to avoid duplicate entries in the cache table
    df['md5'] = df.apply(
        lambda x:
        hashlib.md5(f"{x['street'].upper()} {x['city'].upper()} {x['state'].upper()} {x['zipcode'].upper()}".encode()).hexdigest()
        ,axis=1
    )

    # connect to placekey cache table
    table = TableClient.from_connection_string(
        conn_str=os.environ["ADDRESSES_CONN_STR"], table_name="placekeys"
    )

    # find md5 hashes that already exist in the cache table
    exists_entities = []
    for i, row in df.iterrows():
        entities = table.query_entities(f"RowKey eq '{row['md5']}'")
        [
            exists_entities.append(
                {
                    **entity,
                    "timestamp": entity._metadata["timestamp"],
                    "index":row['index']
                }
            )
            for entity in entities
        ]

    # filter to entries fresher than a set amount of time
    freshness_cutoff = pd.Timestamp(dt.utcnow() - freshness_window, tz="UTC")
    exists = pd.DataFrame(exists_entities)
    if len(exists):
        exists = exists[exists["timestamp"] >= freshness_cutoff]
        exists = exists.drop(columns='timestamp')

    # narrow the list of inputs to get only the ones that don't already exist (and therefore we need to send to placekey)
    if len(exists):
        not_exists = df[df['md5'].apply(lambda x: x not in exists['RowKey'].unique())].reset_index(drop=True)
    else:
        not_exists = df

    # send a bulk request to get placekeys
    url = "https://api.placekey.io/v1/placekeys"
    headers = {
        "apikey": os.environ["PLACEKEY_API_KEY"],
        "Content-Type": "application/json",
    }

    # batch requests with max chunksize of 100
    chunksize = 100
    chunked_data = []
    for chunk in [
        not_exists[i : i + chunksize] 
        for i in range(0, len(not_exists), chunksize)
    ]:
        payload = {
            "queries": [
                {
                    "query_id": str(row["index"]),
                    "street_address": row["street"],
                    "city": row["city"],
                    "region": row["state"],
                    "postal_code": row["zipcode"],
                    "iso_country_code": "US",
                }
                for i, row in chunk.iterrows()
            ]
        }

        r = requests.post(url=url, json=payload, headers=headers)
        print(f"Status: {r.status_code}")

        chunked_data.extend(json.loads(r.content))

    if len(chunked_data):
        new_data = pd.DataFrame(chunked_data)
        new_data["query_id"] = new_data["query_id"].astype(int)
    else:
        new_data = pd.DataFrame()

    # format the output and update the cache table (if necessary)
    if len(new_data):
        # if some new placekeys were run, merge the results back onto the original address data
        m = pd.merge(
            new_data, 
            df, 
            left_on="query_id", 
            right_index=True,
            how='inner'
        ).drop(columns="query_id")
        res = pd.concat([exists.rename(columns={'PartitionKey':'placekey', 'RowKey':'md5'}), m])

        # update cache with new data
        for i, row in m.iterrows():
            table.upsert_entity(
                entity = {
                    "PartitionKey":row['placekey'],
                    "RowKey":row['md5'],
                    **{k:v for k,v in row.to_dict().items() if k in ['street','city','state','zipcode']},
                    # "updated_by":payload['user'] # NOTE re-enable this once authorization is set up
                }
            )
    else:
        # if no new data was run, return any cached data
        res = exists.rename(columns={'PartitionKey':'placekey'})

    return HttpResponse(
        res[['index','placekey']].to_json(orient='records', indent=2),
        status_code=200
    )

class PlacekeyPayload(BaseModel):
    addresses: conlist(AddressComponents2, min_length=1)