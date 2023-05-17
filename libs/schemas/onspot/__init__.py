from .requests.base import GeoJsonBaseSchema, GeoJsonWithSaveSchema, FilesBaseSchema
from .requests.attribution import AttributionSchema, AttributionWithSaveSchema
from .requests.address import AddressWithSaveSchema
from .requests.counts import GeoJsonGroupedByDaySchema, GeoJsonGroupedByIntervalSchema
from .requests.demographics import GeoJsonDemographicsSchema
from .requests.min_max import (
    GeoJsonWithMinMaxSchema,
    GeoJsonWithMinMaxSaveSchema,
    FilesWithMinMaxSaveSchema,
)
from .requests.observations import GeoJsonObservationsWithSaveSchema
from .requests.political import (
    GeoJsonPoliticalAggregateSchema,
    GeoJsonPoliticalWithSaveSchema,
    FilesPoliticalWithSaveSchema,
)
from .requests.publish import DevicesSegment
from .requests.social import GeoJsonSocialsWithSaveSchema
from .requests.trade import GeoJsonTradesWithSaveSchema

__all__ = [
    "AddressWithSaveSchema",
    "AttributionSchema",
    "AttributionWithSaveSchema",
    "GeoJsonBaseSchema",
    "GeoJsonDemographicsSchema",
    "FilesBaseSchema",
    "FilesWithMinMaxSaveSchema",
    "GeoJsonGroupedByDaySchema",
    "GeoJsonObservationsWithSaveSchema",
    "GeoJsonPoliticalAggregateSchema",
    "GeoJsonPoliticalWithSaveSchema",
    "GeoJsonSocialsWithSaveSchema",
    "GeoJsonTradesWithSaveSchema",
    "GeoJsonWithSaveSchema",
    "GeoJsonWithMinMaxSchema",
    "GeoJsonWithMinMaxSaveSchema",
]

endpoints = {
    "callback": {
        "geoframe/all/count": GeoJsonBaseSchema,
        "geoframe/all/countgroupedbyday": GeoJsonGroupedByDaySchema,
        "geoframe/all/countgroupedbydevice": GeoJsonBaseSchema,
        "geoframe/all/countgroupedbyhour": GeoJsonBaseSchema,
        "geoframe/all/countgroupedbyinterval": GeoJsonGroupedByIntervalSchema,
        "geoframe/all/devices": GeoJsonBaseSchema,
        "geoframe/all/observations": GeoJsonBaseSchema,
        "geoframe/all/devicefrequencycount": GeoJsonBaseSchema,
        "geoframe/all/devicelocationsbyday": GeoJsonBaseSchema,
        "geoframe/all/distinctdevicelocations": GeoJsonBaseSchema,
        "geoframe/all/locations": GeoJsonBaseSchema,
        "geoframe/common/count": GeoJsonBaseSchema,
        "geoframe/common/devices": GeoJsonBaseSchema,
        "geoframe/demographics/aggregate": GeoJsonDemographicsSchema,
        "geoframe/demographics/aggregate/zipcodes": GeoJsonWithMinMaxSchema,
        "geoframe/extension/devicesinhome": GeoJsonWithMinMaxSchema,
        "geoframe/extension/devicesinhomecount": GeoJsonWithMinMaxSchema,
        "geoframe/household/count": GeoJsonBaseSchema,
        "geoframe/household/devices": GeoJsonBaseSchema,
        "geoframe/legacyextension/devicesinhome": GeoJsonWithMinMaxSchema,
        "geoframe/political/aggregate": GeoJsonPoliticalAggregateSchema,
        "geoframe/tradearea": GeoJsonTradesWithSaveSchema,
        "geoframes/attribution": AttributionSchema,
        "devices/segment/push": DevicesSegment,
        "geoframe/segment/push": DevicesSegment,
    },
    "storage": {
        "save/addresses/all/devices": AddressWithSaveSchema,
        "save/businessaddresses/all/demographics": AddressWithSaveSchema,
        "save/businessaddresses/all/devices": AddressWithSaveSchema,
        "save/businessaddresses/all/locations": AddressWithSaveSchema,
        "save/files/demographics/all": FilesWithMinMaxSaveSchema,
        "save/files/extension/socialnetwork": FilesPoliticalWithSaveSchema,
        "save/files/political": FilesPoliticalWithSaveSchema,
        "save/geoframe/all/countgroupedbydevice": GeoJsonWithSaveSchema,
        "save/geoframe/all/devices": GeoJsonWithSaveSchema,
        "save/geoframe/all/devicecountataddress": GeoJsonObservationsWithSaveSchema,
        "save/geoframe/all/devicecountatworkplaceaddress": GeoJsonObservationsWithSaveSchema,
        "save/geoframe/all/observations": GeoJsonObservationsWithSaveSchema,
        "save/geoframe/demographics/all": GeoJsonWithMinMaxSaveSchema,
        "save/geoframe/extension/devicesinhome": GeoJsonWithMinMaxSaveSchema,
        "save/geoframe/extension/socialnetwork": GeoJsonSocialsWithSaveSchema,
        "save/geoframe/legacyextension/devicesinhome": GeoJsonWithMinMaxSaveSchema,
        "save/geoframe/political": GeoJsonPoliticalWithSaveSchema,
        "save/geoframe/tradearea": GeoJsonTradesWithSaveSchema,
        "save/geoframes/attribution/devices": AttributionWithSaveSchema,
        "save/geoframes/attribution/journey": AttributionWithSaveSchema,
        "save/geoframes/attribution/locations": AttributionWithSaveSchema,
        "save/files/extension/devicesinhome": FilesBaseSchema,
    }
}
