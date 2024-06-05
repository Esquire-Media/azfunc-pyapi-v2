# File: libs/azure/functions/blueprints/esquire/audiences/ingress/mover_sync/activities/validate_address_chunks.py

from libs.azure.functions import Blueprint
from azure.data.tables import TableClient
from datetime import (
    datetime as dt,
    timedelta,
    timezone,
)
import hashlib, os, pandas as pd, requests, json

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()

freshness_window = timedelta(days=365)


# Define an activity function
@bp.activity_trigger(input_name="ingress")
def activity_placekey_fromAddressBatch(ingress: list):
    """
    ingress:
        [
            {
                "street":"123 Main St",
                "city":"Chicago",
                "state":"IL",
                "zipcode":"12345"
            },
            ...
        ]

    return:
    [
        "228@647-5c5-7wk",
        ...
    ]
    """

    # add md5 hashes to each record for unique indexing purposes
    for i, x in enumerate(ingress): 
        x['md5'] = hashlib.md5(f"{x['street'].upper()} {x['city'].upper()} {x['state'].upper()} {x['zipcode'].upper()}".encode()).hexdigest()
        x['index'] = i
    ingress[-1]

    # connect to placekey cache table
    table = TableClient.from_connection_string(
        conn_str=os.environ["ADDRESSES_CONN_STR"], table_name="placekeys"
    )

    # collect data for md5 hashes that already exist in the cache table
    cached_entities = []
    indexes_to_pop = []
    for i, x in enumerate(ingress):
        entities = table.query_entities(f"RowKey eq '{x['md5']}'")
        for entity in entities: # there should only ever be one entity returned, but we make this a loop as a failsafe just in case
            # collect the cached data if it falls was updated within the freshness window
            if entity._metadata["timestamp"] >= dt.utcnow().replace(tzinfo=timezone.utc) - freshness_window:
                cached_entities.append({
                    "index":x['index'],
                    "placekey":entity['PartitionKey'],
                    # "timestamp": entity._metadata["timestamp"]
                })
                # once cache data is retrieved, remove the address from the to_send list
                indexes_to_pop.append(i)

    ingress = [x for i, x in enumerate(ingress) if i not in indexes_to_pop]

    # send a bulk request to get placekeys
    url = "https://api.placekey.io/v1/placekeys"
    headers = {
        "apikey": os.environ["PLACEKEY_API_KEY"],
        "Content-Type": "application/json",
    }

    # batch requests with max chunksize of 100
    chunksize = 100
    new_entities = []
    for chunk in [ingress[i : i + chunksize] for i in range(0, len(ingress), chunksize)]:
        # build the payload and send to the placekey endpoint
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
                for row in chunk
            ]
        }
        r = requests.post(url=url, json=payload, headers=headers)
        js = json.loads(r.content)

        # format the new entities for function return
        [
            new_entities.append(
                {
                    "index": int(x["query_id"]),
                    "placekey": x["placekey"],
                }
            )
            for x in js
        ]

    # update the table cache with any newly queried placekeys
    if len(new_entities):
        to_write = (
            pd.merge(pd.DataFrame(ingress), pd.DataFrame(new_entities), on="index")
            .drop(columns=["index"])
            .to_dict(orient="records")
        )
        for x in to_write:
            table.upsert_entity(
                {
                    "PartitionKey":x['placekey'],
                    "RowKey":x['md5'],
                    **{k:v for k,v in x.items() if k in ['street','city','state','zipcode']}
                }
            )


    # combine the new data with the cached data, sort, and return as a flat list of placekeys
    res = new_entities + cached_entities
    res.sort(key=lambda x: x["index"])
    return [x["placekey"] for x in res]