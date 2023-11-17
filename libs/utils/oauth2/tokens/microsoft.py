from . import TokenValidationError, ValidateWellKnown


class ValidateMicrosoft(ValidateWellKnown):
    """
    A subclass of ValidateWellKnown specifically tailored for Microsoft's OpenID Connect implementation.
    This class is designed to validate JWTs issued by Microsoft identity platforms like Azure AD.
    It allows additional customization for different Microsoft authority URLs and tenant IDs.
    """

    def __init__(
        self, authority: str, tenant_id: str, client_id: str, version: str = "v2.0"
    ):
        """
        Initialize the Microsoft-specific JWT validator.

        Parameters
        ----------
        authority : str
            The base URL of the Microsoft authority (e.g., login.microsoftonline.com).
        tenant_id : str
            The tenant ID of the Azure AD instance.
        client_id : str
            The client ID of the application that should match the 'aud' claim in the token.
        version : str, optional
            The version of the Microsoft identity platform API. Defaults to 'v2.0'.
        """
        self.client_id = client_id
        well_known_url = f"https://{authority}/{tenant_id}/{version}/.well-known/openid-configuration"
        super().__init__(well_known_url)

    def additional_token_validation(self, payload: dict):
        """
        Perform additional validation specific to Microsoft-issued tokens.
        This method checks if the 'aud' (audience) claim in the token matches the provided client ID.
        """
        if self.client_id and payload["aud"] != self.client_id:
            raise TokenValidationError(
                "Audience in the provided token ({}) does not match the expected value of ({})".format(
                    payload["aud"], self.client_id
                )
            )


class ValidateMicrosoftOnline(ValidateMicrosoft):
    """
    A specialized subclass of ValidateMicrosoft for validating JWTs issued for Microsoft Entra registered applications.
    This class is specifically designed for applications registered with Microsoft Entra, providing streamlined setup.
    """

    def __init__(self, tenant_id: str, client_id: str):
        """
        Initialize the validator for Microsoft Entra registered applications.

        Parameters
        ----------
        tenant_id : str
            The tenant ID of the Azure AD instance.
        client_id : str
            The client ID of the registered application.
        """
        super().__init__("login.microsoftonline.com", tenant_id, client_id)


class ValidateMicrosoftWindows(ValidateMicrosoft):
    """
    A specialized subclass of ValidateMicrosoft for validating JWTs specifically issued by Microsoft Graph.
    This class is optimized for scenarios involving interaction with Microsoft Graph API.
    """

    def __init__(
        self, tenant_id: str, client_id: str = "00000003-0000-0000-c000-000000000000"
    ):
        """
        Initialize the validator for Microsoft Graph.

        Parameters
        ----------
        tenant_id : str
            The tenant ID of the Azure AD instance.
        client_id : str, optional
            The client ID of the application, defaulting to a common client ID used by Microsoft Graph.
        """
        super().__init__("sts.windows.net", tenant_id, client_id)