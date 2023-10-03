from libs.openapi.clients.gravity_forms import GravityFormsAPI
from libs.openapi.clients.meta import Meta
from libs.openapi.clients.microsoft.graph import MicrosoftGraph
from libs.openapi.clients.onspot import OnSpotAPI
from libs.openapi.clients.xandr import XandrAPI

specifications = {
    "GravityForms": GravityFormsAPI.get_spec,
    "Meta": Meta.load,
    "MicrosoftGraph": MicrosoftGraph.load,
    "OnSpot": OnSpotAPI.get_spec,
    "Xandr": XandrAPI.get_spec,
}
