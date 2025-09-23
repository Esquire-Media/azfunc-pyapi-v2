# File: /libs/azure/functions/blueprints/esquire/audiences/egress/meta/utils.py

import os
from typing import Optional
from facebook_business.api import FacebookAdsApi


class MissingCredentialError(RuntimeError):
    pass


def _resolve_credential(value: Optional[str]) -> Optional[str]:
    """
    Deterministically resolve a provided credential value.

    Rules:
      - If the provided value is None/empty -> return None.
      - If the value starts with 'env:' -> treat the remainder as an env var name.
      - Otherwise -> treat the value as the literal secret/token (use verbatim).
    """
    if not value:
        return None
    if isinstance(value, str) and value.startswith("env:"):
        return os.environ.get(value[4:])
    return value


def get_api_credential(key: str, ingress: dict, env_var: str) -> Optional[str]:
    """
    Deterministically pick a credential source.

    Priority:
      1) ingress[key] (verbatim, unless prefixed with 'env:' to opt-in to env lookup)
      2) os.environ[env_var]
    """
    if ingress and key in ingress:
        resolved = _resolve_credential(ingress.get(key))
        if resolved:
            return resolved
    return os.environ.get(env_var)


def initialize_facebook_api(ingress: dict) -> FacebookAdsApi:
    """
    Initializes the Facebook Ads API using deterministic credential resolution.
    """
    access_token = get_api_credential("access_token", ingress, "META_ACCESS_TOKEN")
    app_id = get_api_credential("app_id", ingress, "META_APP_ID")
    app_secret = get_api_credential("app_secret", ingress, "META_APP_SECRET")

    if not access_token or not app_id or not app_secret:
        raise MissingCredentialError(
            "Missing Meta API credentials. Provide them in ingress "
            "(optionally as 'env:VAR_NAME') or set META_ACCESS_TOKEN, META_APP_ID, META_APP_SECRET."
        )

    return FacebookAdsApi.init(
        access_token=access_token,
        app_id=app_id,
        app_secret=app_secret,
    )
