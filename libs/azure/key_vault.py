from functools import lru_cache
from azure.keyvault.secrets import SecretClient
from .credentials import GetCredential


@lru_cache(maxsize=8)
def KeyVaultClient(vault_name):
    """
    Access the Azure Key Vault to load needed secret tokens.

    Cached per vault name to prevent SNAT port exhaustion.
    """
    secret_client = SecretClient(
        vault_url=f"https://{vault_name}.vault.azure.net/",
        credential=GetCredential()
    )
    return secret_client