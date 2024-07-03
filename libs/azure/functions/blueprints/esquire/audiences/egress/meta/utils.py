from facebook_business.api import FacebookAdsApi
import os

def get_api_credential(key, ingress, env_var):
    """
    Retrieves the API credential from the ingress dictionary or environment variable.

    Args:
        key (str): The key to look for in the ingress dictionary.
        ingress (dict): The ingress dictionary containing API credentials.
        env_var (str): The environment variable to look for if the key is not present in ingress.

    Returns:
        str: The API credential value.
    """
    return (
        os.environ.get(ingress[key], ingress[key])
        if ingress.get(key)
        else os.environ.get(env_var)
    )


def initialize_facebook_api(ingress):
    """
    Initializes the Facebook Ads API with credentials from the ingress dictionary or environment variables.

    Args:
        ingress (dict): A dictionary containing the API credentials:
            - "access_token" (optional, str): The access token for Facebook API. If not provided, it will be fetched from environment variables.
            - "app_id" (optional, str): The app ID for Facebook API. If not provided, it will be fetched from environment variables.
            - "app_secret" (optional, str): The app secret for Facebook API. If not provided, it will be fetched from environment variables.

    Returns:
        FacebookAdsApi: The initialized Facebook Ads API instance.
    """
    access_token = get_api_credential("access_token", ingress, "META_ACCESS_TOKEN")
    app_id = get_api_credential("app_id", ingress, "META_APP_ID")
    app_secret = get_api_credential("app_secret", ingress, "META_APP_SECRET")

    return FacebookAdsApi.init(
        access_token=access_token, app_id=app_id, app_secret=app_secret
    )