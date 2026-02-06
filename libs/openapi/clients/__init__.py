from libs.openapi.clients.meta import Meta
from libs.openapi.clients.microsoft.graph import MicrosoftGraph
from libs.openapi.clients.onspot import OnSpotAPI

specifications = {
    "Meta": Meta.load,
    "MicrosoftGraph": MicrosoftGraph.load,
    "OnSpot": OnSpotAPI.get_spec,
}
