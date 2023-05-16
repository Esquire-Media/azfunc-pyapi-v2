from .base import GeoJsonBaseSchema, GeoJsonWithSaveSchema
from .attribution import AttributionSchema, AttributionWithSaveSchema
from .address import AddressSchema
from .counts import GeoJsonGroupedByDaySchema, GeoJsonGroupedByIntervalSchema
from .demographics import GeoJsonDemographicsSchema
from .min_max import (
    GeoJsonWithMinMaxSchema,
    GeoJsonWithMinMaxSaveSchema,
    FilesWithMinMaxSaveSchema,
)
from .observations import GeoJsonObservationsWithSaveSchema
from .political import (
    GeoJsonPoliticalAggregateSchema,
    GeoJsonPoliticalWithSaveSchema,
    FilesPoliticalWithSaveSchema,
)
from .social import GeoJsonSocialsWithSaveSchema
from .trade import GeoJsonTradesWithSaveSchema

__all__ = [
    "AddressSchema",
    "AttributionSchema",
    "AttributionWithSaveSchema",
    "GeoJsonBaseSchema",
    "GeoJsonDemographicsSchema",
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
        #"devices/segment/push" ,
        #"geoframe/segment/push": ,
    },
    "stored": {
        "save/addresses/all/devices": AddressSchema,
        "save/businessaddresses/all/demographics": AddressSchema,
        "save/businessaddresses/all/devices": AddressSchema,
        "save/businessaddresses/all/locations": AddressSchema,
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
        # "save/files/extension/devicesinhome": ,
    }
}
