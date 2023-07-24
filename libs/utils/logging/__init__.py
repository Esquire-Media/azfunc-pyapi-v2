from .handlers.azure_table import AzureTableHandler
from .handlers.local import LocalFileHandler

"""
import logging
from libs.utils.logging import AzureTableHandler

# Create a logger
logger = logging.getLogger("example_logger")
logger.setLevel(logging.DEBUG)

# Create an instance of the custom handler
custom_handler = AzureTableHandler()
custom_handler.setLevel(logging.DEBUG)

# Create a formatter and set it on the custom handler
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
custom_handler.setFormatter(formatter)

# Add the custom handler to the logger
logger.addHandler(custom_handler)
"""

__all__ = ["LocalFileHandler", "AzureTableHandler"]
