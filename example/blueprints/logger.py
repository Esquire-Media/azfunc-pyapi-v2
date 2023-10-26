from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest
from azure.durable_functions import DurableOrchestrationClient
import logging
from libs.utils.logging import AzureTableHandler
from uuid import uuid4


bp = Blueprint()

@bp.logger
@bp.route(route="starter", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def http_default_logger(req: HttpRequest, client: DurableOrchestrationClient):
    """
    Example HTTP starter with the default logger object.
    Logs will be automatically recorded in a `logging` table in the AzureWebJobsStorage storage account.
    """
    pass

# -------

# initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("custom.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)

@bp.route(route="starter", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def http_custom_logger(req: HttpRequest, client: DurableOrchestrationClient):
    """
    Example HTTP starter with a custom logger object.
    Logs will be automatically recorded in a `logging` table in the AzureWebJobsStorage storage account.
    """
    logger = logging.getLogger("custom.logger")
    logger.info(msg="logged successfully!", extra={"context":{"PartitionKey":"index", "RowKey":uuid4(),"CustomField":"custom_value"}})