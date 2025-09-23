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
                - "batch_seq" (int): 1-based batch sequence number (required by FB sessions).
                - "session_id" (int): The session ID for the batch process.
                - "estimated_num_total" (int): Total distinct records expected.
                - "last_batch_flag" (bool): True for the last batch in the session.
            - "batch_size" (int): Number of records per page.
            - "access_token" (optional, str)
            - "app_id" (optional, str)
            - "app_secret" (optional, str)

    Returns:
        dict: The FB response JSON or an object describing why a call was skipped.
    """
    try:
        # Convert 1-based FB session sequence to 0-based SQL OFFSET
        page_index_zero_based = max(ingress["batch"]["batch_seq"] - 1, 0)
        offset = page_index_zero_based * ingress["batch_size"]
        limit = ingress["batch_size"]

        # Page the distinct MAIDs deterministically
        df = pd.read_sql(
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
                offset,
                limit,
            ),
            from_bind(ingress["sql"]["bind"]).connect().connection(),
        )

        users = (
            df["deviceid"]
            .astype(str)
            .str.lower()
            .tolist()
            if not df.empty
            else []
        )

        # If this page is empty, do not call the API (prevents (#100))
        if not users:
            return {
                "skipped": True,
                "reason": "Empty batch; no users for this page.",
                "batch": ingress["batch"],
                "offset": offset,
                "limit": limit,
            }

        return (
            CustomAudience(
                fbid=ingress["audience"]["audience"],
                api=initialize_facebook_api(ingress),
            )
            .add_users(
                schema=CustomAudience.Schema.mobile_advertiser_id,
                is_raw=True,
                users=users,
                session=ingress["batch"],
            )
            .json()
        )
    except FacebookRequestError as e:
        return e.body()
