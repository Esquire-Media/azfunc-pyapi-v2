from libs.openapi.clients.gravity_forms import GravityFormsAPI
from libs.openapi.clients.meta import MetaAPI
from libs.openapi.clients.onspot import OnSpotAPI
from libs.openapi.clients.xandr import XandrAPI

specifications = {
    "GravityForms": GravityFormsAPI.get_spec,
    "Meta": MetaAPI.get_spec,
    "OnSpot": OnSpotAPI.get_spec,
    "Xandr": XandrAPI.get_spec,
}
