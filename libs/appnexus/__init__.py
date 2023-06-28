import requests
import json
import time


class AppNexusAuth(requests.auth.AuthBase):
    """
    AppNexus authentication handler for requests.

    This class provides authentication for the AppNexus API by obtaining and managing authentication tokens.

    Parameters
    ----------
    username : str
        The username for authentication.
    password : str
        The password for authentication.

    Attributes
    ----------
    username : str
        The username for authentication.
    password : str
        The password for authentication.
    base_url : str
        The base URL for the AppNexus API.
    _token : str or None
        The authentication token obtained from the API. None if not yet obtained.
    last_auth_time : float
        The timestamp of the last authentication request.

    Notes
    -----
    - This class extends `requests.auth.AuthBase` and can be used as an authentication handler in the `requests` library.
    - The authentication token is automatically renewed every 2 hours.

    Examples
    --------
    Create an instance of `AppNexusAuth`:

    >>> auth = AppNexusAuth("my_username", "my_password")
    """

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.base_url = "https://api.appnexus.com/"
        self._token = None
        self.last_auth_time = 0

    @property
    def token(self):
        """
        Get the authentication token.

        This property retrieves and returns the authentication token.
        If a valid token is already available, it is returned.
        Otherwise, a new token is obtained by making an authentication request to the AppNexus API.

        Returns
        -------
        str or None
            The authentication token, or None if token retrieval failed.

        Raises
        ------
        requests.exceptions.RequestException
            If the authentication request fails.
        """
        current_time = time.time()
        if current_time - self.last_auth_time > 7200:  # 2 hours = 7200 seconds
            auth_url = self.base_url + "auth"
            auth_data = {"auth": {"username": self.username, "password": self.password}}
            response = requests.post(
                auth_url,
                data=json.dumps(auth_data),
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()  # Raise exception if the request failed
            self._token = response.json().get("response").get("token")
            self.last_auth_time = current_time
        return self._token

    def __call__(self, r: requests.Request):
        """
        Modify the request object before sending it.

        This method is invoked when an instance of `AppNexusAuth` is called as a function.
        It modifies the request object by adding the authentication token and content type 
        to the headers.

        Parameters
        ----------
        r : requests.Request
            The request object to be modified.

        Returns
        -------
        requests.Request
            The modified request object.
        """
        r.headers["Authorization"] = self.token
        r.headers["Content-Type"] = "application/json"
        return r
