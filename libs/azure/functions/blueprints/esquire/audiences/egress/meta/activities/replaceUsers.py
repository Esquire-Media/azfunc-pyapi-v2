# File: /libs/azure/functions/blueprints/esquire/audiences/meta/activities/replaceUsers.py

from azure.durable_functions import Blueprint
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.customaudience import CustomAudience
from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import (
    initialize_facebook_api,
)
from libs.data import from_bind
import pandas as pd

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudience_replaceUsers(ingress: dict):
    """
    Replaces users in a Facebook Custom Audience using the provided ingress details.

    Args:
        ingress (dict): A dictionary containing the following keys:
            - "audience" (dict): Contains interal audience meta data.
                - "id" (str): The internal audience ID
                - "audience" (str): The Meta audience ID
            - "sql" (dict): Contains the SQL query and bind details for retrieving user device IDs:
                - "query" (str): The SQL query to fetch device IDs.
                - "bind" (str): The database connection bind string.
            - "batch" (dict): Contains batching details:
                - "sequence" (int): The current batch sequence number.
                - "size" (int): The number of records to fetch per batch.
                - "total" (int): The total number of records to process.
                - "session_id" (optional, int): The session ID for the batch process. If not provided, a random session ID will be generated.
            - "access_token" (optional, str): The access token for Facebook API. If not provided, it will be fetched from environment variables.
            - "app_id" (optional, str): The app ID for Facebook API. If not provided, it will be fetched from environment variables.
            - "app_secret" (optional, str): The app secret for Facebook API. If not provided, it will be fetched from environment variables.

    Returns:
        None
    """
    try:
        return (
            CustomAudience(
                fbid=ingress["audience"]["audience"],
                api=initialize_facebook_api(ingress),
            )
            .add_users(
                schema=CustomAudience.Schema.mobile_advertiser_id,
                is_raw=True,
                users=pd.read_sql(
                    """
                        {}
                        ORDER BY deviceid
                        OFFSET {} ROWS
                        FETCH NEXT {} ROWS ONLY
                    """.format(
                        ingress["sql"]["query"].format(
                            ingress["destination"]["container_name"],
                            ingress["destination"]["blob_prefix"],
                            ingress["destination"]["data_source"],
                        ),
                        ingress["batch"]["batch_seq"] * ingress["batch_size"],
                        ingress["batch_size"],
                    ),
                    from_bind(ingress["sql"]["bind"]).connect().connection(),
                )["deviceid"]
                .apply(lambda x: str(x).lower())
                .to_list(),
                session=ingress["batch"],
            )
            .json()
        )
    except FacebookRequestError as e:
        return e.body()
