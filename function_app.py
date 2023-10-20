from azure.functions import AuthLevel
from libs.azure.functions import FunctionApp
from deployment import get_bps
import logging

logger = logging.getLogger('azure')
logger.setLevel(logging.WARNING)

app = FunctionApp(http_auth_level=AuthLevel.ANONYMOUS)

app.register_blueprints(get_bps(debug=False))
