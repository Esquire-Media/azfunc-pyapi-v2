from . import ValidateWellKnown


class ValidateMicrosoft(ValidateWellKnown):
    """
    A subclass of ValidateWellKnown specifically tailored for Microsoft's OpenID Connect implementation.
    This class is designed to validate JWTs issued by Microsoft identity platforms like Azure AD.
    It allows additional customization for different Microsoft authority URLs and tenant IDs.
    """

    def __init__(
        self, tenant_id: str, client_id: str, authority: str = "login.microsoftonline.com", version: str = None
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
        super().__init__(
            openid_config_url="https://{}/{}{}/.well-known/openid-configuration".format(
                authority,
                tenant_id,
                "/" + version if version else "",
            ),
            audience=client_id,
        )
