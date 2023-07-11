from requests.models import urlencode  # type: ignore
from typing import Optional, List

import requests


class Client:
    def __init__(
        self, OpenIDConfig: str, client_id: str, client_secret: str, redirect_uri: str
    ):
        self.config = requests.get(OpenIDConfig).json()
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri

    def get_auth_request_url(self, scopes: Optional[List[str]] = None) -> str:
        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "redirect_uri": self.redirect_uri,
        }
        if scopes:
            params["scope"] = " ".join(scopes)
        formatted_params = "".join(urlencode(params))
        return f'{self.config["authorization_endpoint"]}?{formatted_params}'

    def authorize(
        self,
        grant_type: str,
        code_type: str = None,
        code: str = None,
        scopes: Optional[List[str]] = None,
    ):
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": grant_type,
            "redirect_uri": self.redirect_uri
        }
        if code_type and code:
            payload[code_type] = code
        if scopes:
            payload["scope"] = " ".join(scopes)
            
        response = requests.post(self.config["token_endpoint"], payload)
        if response.status_code == 200:
            content = response.json()

            for key, value in content.items():
                setattr(self,key,value)
