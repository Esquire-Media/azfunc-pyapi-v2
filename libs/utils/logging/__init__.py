from .handlers.azure_table import AzureTableHandler
from .handlers.local import LocalFileHandler

__all__ = [
    "LocalFileHandler",
    "AzureTableHandler"
]