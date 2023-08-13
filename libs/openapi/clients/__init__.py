from libs.openapi.clients.gravity_forms import GravityFormsAPI
from libs.openapi.clients.meta import MetaAPI
from libs.openapi.clients.onspot import OnSpotAPI
from libs.openapi.clients.xandr import XandrAPI

specifications = {
    "GravityForms": GravityFormsAPI.spec,
    "Meta": MetaAPI.spec,
    "OnSpot": OnSpotAPI.spec,
    "Xandr": XandrAPI.spec,
}
