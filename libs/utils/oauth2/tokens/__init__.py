from abc import ABC, abstractmethod
from jwt.algorithms import RSAAlgorithm
from typing import Any
import httpx, jwt


class TokenValidationError(Exception):
    """
    Custom exception for handling token validation errors.
    This exception is raised when the JWT does not meet expected validation criteria.
    """

    pass


class ValidateGeneric(ABC):
    """
    Abstract base class for OAuth token validation.

    Methods
    -------
    __call__(token)
        Validates the given OAuth token.

    validate_token(token)
        Abstract method to be implemented by subclasses for token validation.

    get_key(jwks, kid)
        Retrieves and formats the key from JWKS.
    """

    def __call__(self, token):
        """
        Validates the given OAuth token using the validate_token method.

        Parameters
        ----------
        token : str
            The OAuth token to be validated.

        Returns
        -------
        dict or str
            Decoded token information if validation is successful, or error message.
        """
        return self.validate_token(token)

    def get_matching_key(self, keys: list, kid: str):
        """
        Retrieve the public key that matches the 'kid' from the JWKS endpoint.
        This key is used for verifying the JWT's signature.
        """
        matching_key = next((key for key in keys if key["kid"] == kid), None)
        if not matching_key:
            raise TokenValidationError("Public key for validation not found.")
        return RSAAlgorithm.from_jwk(matching_key)

    @abstractmethod
    def validate_token(self, token):
        """
        Abstract method for token validation. Must be implemented by subclasses.

        Parameters
        ----------
        token : str
            The OAuth token to be validated.

        Raises
        ------
        NotImplementedError
            If the method is not implemented in the subclass.
        """
        raise NotImplementedError("Subclasses must implement validate_token method")


class ValidateWellKnown(ValidateGeneric):
    """
    A JWT validator class that implements OAuthTokenValidator for validating JWT tokens.

    Inherits all parameters and methods from OAuthTokenValidator.

    Parameters
    ----------
    openid_config_url : str
        URL to the OpenID configuration.
    audience : str
        The audience value expected in the token.
    http_client : callable, optional
        HTTP client used for making requests. Defaults to httpx.Client.
    """

    def __init__(self, openid_config_url, audience=None, http_client=httpx.Client):
        self.audience = audience
        self.http_client = http_client()
        self.openid_config = self.http_client.get(url=openid_config_url).json()
        self.public_keys = self.http_client.get(
            url=self.openid_config["jwks_uri"]
        ).json()["keys"]

    def fetch_openid_config(self, url: str):
        self.http_client.get(url).json()

    def validate_token(self, token):
        """
        Validates a JWT token using OpenID and JWKS information.

        Parameters
        ----------
        token : str
            The JWT token to be validated.

        Returns
        -------
        dict or str
            Decoded token information if validation is successful, or error message.
        """
        # Strip Bearer from the beginning if it's there
        if token[:7] == "Bearer ":
            token = token[7:]
        # Decode the token header without validation
        try:
            unverified_header = jwt.get_unverified_header(token)
        except jwt.InvalidTokenError as e:
            return f"Error decoding token header: {e}"

        # Validate Token
        return jwt.decode(
            token,
            key=self.get_matching_key(self.public_keys, unverified_header["kid"]),
            algorithms=[unverified_header["alg"]],
            audience=self.audience,
        )
