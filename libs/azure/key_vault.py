from azure.keyvault.secrets import SecretClient
from .credentials import GetCredential

def KeyVaultClient(vault_name):
    """
    Access the Azure Key Vault to load needed secret tokens.
    """
    secret_client = SecretClient(
        vault_url=f"https://{vault_name}.vault.azure.net/",
        credential=GetCredential()
    )
    return secret_client