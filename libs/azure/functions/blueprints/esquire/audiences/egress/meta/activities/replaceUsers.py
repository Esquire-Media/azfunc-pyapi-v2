from __future__ import annotations

from typing import Any, Dict, List

from azure.durable_functions import Blueprint
from facebook_business.exceptions import FacebookRequestError
from facebook_business.adobjects.customaudience import CustomAudience

from libs.azure.functions.blueprints.esquire.audiences.egress.meta.blobReader import (
    fetch_users_page,
)
from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import (
    initialize_facebook_api,
)

bp = Blueprint()


def _meta_users_replace(ingress: Dict[str, Any], users: List[str]) -> Dict[str, Any]:
    """
    Calls Meta usersreplace with stable session semantics using the SDK's adobject method.
    This path is known-good in your environment (unlike api.call which was missing scheme).
    """
    api = initialize_facebook_api(ingress)
    audience_id = ingress["audience"]["audience"]

    # Build params with minimal extra copying.
    params = {
        "payload": {
            "schema": CustomAudience.Schema.mobile_advertiser_id,
            "data": users,
        },
        "session": ingress["batch"],
    }

    result = CustomAudience(fbid=audience_id, api=api).create_users_replace(
        params=params
    )

    # Response is small; exporting is fine and keeps the activity output JSON-serializable.
    return result.export_all_data()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudience_replaceUsers(ingress: dict):
    """
    Sends a deterministic REPLACE batch to Meta using the usersreplace endpoint,
    reading the batch straight from the audience blobs.

    Memory reductions:
      - Read only the requested byte range of the audience blob.
      - Drop the users list immediately after the request.
    """
    users, blob_error, offset, limit = fetch_users_page(ingress)

    if blob_error is not None:
        return blob_error

    assert users is not None

    # If this page is empty, do not call the API (prevents (#100) and ensures idempotence)
    if not users:
        return {
            "skipped": True,
            "reason": "Empty batch; no users for this page.",
            "batch": ingress["batch"],
            "offset": offset,
            "limit": limit,
        }

    try:
        return {
            **_meta_users_replace(ingress, users),
            "diag": {
                "users": len(users),
                "offset": offset,
                "limit": limit,
                "sample": users[0:10],
            },
        }
    except FacebookRequestError as e:
        body = e.body() if hasattr(e, "body") else {"error": {"message": str(e)}}
        return {"error": body}
    finally:
        # Drop the largest per-invocation structure ASAP.
        try:
            users.clear()
        except Exception:
            pass
