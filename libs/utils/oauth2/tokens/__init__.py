from abc import ABC, abstractmethod
from jwt.algorithms import RSAAlgorithm
from typing import Any, Callable
import httpx, jwt


class TokenValidationError(Exception):
    """
    Custom exception for handling token validation errors.
    This exception is raised when the JWT does not meet expected validation criteria.
    """

    pass


class ValidateGeneric(ABC):
    """
    Abstract base class designed to validate JWTs using OpenID Connect configurations.
    This class provides foundational methods required for JWT validation and decoding.
    It should be subclassed for specific OpenID Connect provider implementations.
    """

    def __init__(self, openid_config: dict, http_get: Callable = httpx.get):
        """
        Initialize the class with OpenID Connect configuration and an optional HTTP GET function.

        Parameters
        ----------
        openid_config : dict
            Configuration settings required for token validation, including keys and issuer information.
        http_get : Callable, optional
            Function to perform HTTP GET requests. Defaults to httpx.get.
            This function should return a response object with a .json() method.
        """
        self.openid_config = openid_config
        self.http_get = http_get

    def decode_jwt(self, bearer_token: str) -> dict:
        """
        Decode JWT without verifying its signature.
        This method is typically used to extract the header to determine the key for signature verification.
        """
        return jwt.decode(
            bearer_token,
            algorithms=[self.get_unverified_header(bearer_token)["alg"]],
            options={"verify_signature": False},
        )

    def get_unverified_header(self, token: str) -> dict:
        """
        Retrieve the unverified header of the JWT.
        This header contains metadata about the token, such as the algorithm used for signing.
        """
        return jwt.get_unverified_header(token)

    def retrieve_matching_key(self, kid: str) -> dict:
        """
        Retrieve the public key that matches the 'kid' from the JWKS endpoint.
        This key is used for verifying the JWT's signature.
        """
        keys = self.http_get(self.openid_config["jwks_uri"]).json()["keys"]
        matching_key = next((key for key in keys if key["kid"] == kid), None)
        if not matching_key:
            raise TokenValidationError("Public key for validation not found.")
        return matching_key

    @abstractmethod
    def additional_token_validation(self, payload: dict):
        """
        Abstract method for additional processing on the decoded JWT payload.
        This method should be implemented in subclasses to perform provider-specific validations.
        """
        pass

    def __call__(self, bearer_token: str) -> Any:
        """
        Validate a JWT and return its decoded payload if the validation is successful.
        This method serves as the primary interface for JWT validation,
        combining header retrieval, signature verification, and custom processing.
        """
        if bearer_token.startswith("Bearer "):
            bearer_token = bearer_token[7:]

        unverified_header = self.get_unverified_header(bearer_token)
        unverified_payload = self.decode_jwt(bearer_token)

        if unverified_payload["iss"] != self.openid_config["issuer"]:
            raise TokenValidationError(
                "Issuer of the token ({}) is not the expected value ({}).".format(
                    unverified_payload["iss"], self.openid_config["issuer"]
                )
            )

        pem_key = RSAAlgorithm.from_jwk(
            self.retrieve_matching_key(unverified_header["kid"])
        )

        self.additional_token_validation(unverified_payload)

        return jwt.decode(
            bearer_token,
            algorithms=[unverified_header["alg"]],
            key=pem_key,
            audience=unverified_payload["aud"],
            options={"verify_signature": True},
        )


class ValidateWellKnown(ValidateGeneric):
    """
    A subclass of ValidateGeneric designed for validating JWTs using the OpenID Connect discovery document.
    This class fetches configuration from a well-known URL compliant with the OpenID Connect discovery specifications.
    It's suitable for general use cases where OpenID providers publish their configurations at a standard URL.
    The flexibility to provide a custom HTTP GET function makes it adaptable for various environments and testing scenarios.
    """

    def __init__(self, well_known_url: str, http_get: Callable = httpx.get):
        """
        Initialize the validator with the URL of the OpenID Connect discovery document and an optional HTTP GET function.

        Parameters
        ----------
        well_known_url : str
            The URL of the OpenID Connect discovery document (.well-known/openid-configuration).
        http_get : Callable, optional
            Function to perform HTTP GET requests. Defaults to httpx.get.
            This function should return a response object with a .json() method.
            It allows for custom handling of HTTP requests, such as adding special headers or using a different HTTP client.
        """
        super().__init__(http_get(well_known_url).json())
