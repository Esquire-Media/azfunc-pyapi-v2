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
from .responses import ResponseBaseSchema, ResponseWithSaveSchema
from .callbacks.attribution import AttributionCallbackSchema
from .callbacks.base import CallbackBaseSchema, CallbackInfoSchema
from .callbacks.counts import (
    CountCallbackSchema,
    CountByDayCallbackSchema,
    CountByDeviceCallbackSchema,
    CountByHourCallbackSchema,
    CountByIntervalCallbackSchema,
    CountByFrequencyCallbackSchema,
)
from .callbacks.demographics import DemographicZipcodesCallbackSchema
from .callbacks.devices import DevicesCallbackSchema, DeviceLocationsCallbackSchema
from .callbacks.observations import ObservationsCallbackSchema

__all__ = [
    # Request Schemas
    "AddressWithSaveSchema",
    "AttributionSchema",
    "AttributionWithSaveSchema",
    "DevicesSegment",
    "GeoJsonBaseSchema",
    "GeoJsonDemographicsSchema",
    "FilesBaseSchema",
    "FilesWithMinMaxSaveSchema",
    "GeoJsonGroupedByDaySchema",
    "GeoJsonGroupedByIntervalSchema",
    "GeoJsonObservationsWithSaveSchema",
    "GeoJsonPoliticalAggregateSchema",
    "GeoJsonPoliticalWithSaveSchema",
    "GeoJsonSocialsWithSaveSchema",
    "GeoJsonTradesWithSaveSchema",
    "GeoJsonWithSaveSchema",
    "GeoJsonWithMinMaxSchema",
    "GeoJsonWithMinMaxSaveSchema",
    # Response Schemas
    "ResponseBaseSchema",
    "ResponseWithSaveSchema",
    # Callback Schemas,
    "CallbackBaseSchema",
    "CallbackInfoSchema",
    "AttributionCallbackSchema",
    "CountCallbackSchema",
    "CountByDayCallbackSchema",
    "CountByDeviceCallbackSchema",
    "CountByHourCallbackSchema",
    "CountByIntervalCallbackSchema",
    "CountByFrequencyCallbackSchema",
    "DemographicZipcodesCallbackSchema",
    "DevicesCallbackSchema",
    "DeviceLocationsCallbackSchema",
    "ObservationsCallbackSchema",
]

endpoints = {
    "callback": {
        "geoframe/all/count": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": CountCallbackSchema,
        },
        "geoframe/all/countgroupedbyday": {
            "request": GeoJsonGroupedByDaySchema,
            "response": ResponseBaseSchema,
            "callback": CountByDayCallbackSchema,
        },
        "geoframe/all/countgroupedbydevice": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": CountByDeviceCallbackSchema,
        },
        "geoframe/all/countgroupedbyhour": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": CountByHourCallbackSchema,
        },
        "geoframe/all/countgroupedbyinterval": {
            "request": GeoJsonGroupedByIntervalSchema,
            "response": ResponseBaseSchema,
            "callback": CountByIntervalCallbackSchema,
        },
        "geoframe/all/devicefrequencycount": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": CountByFrequencyCallbackSchema,
        },
        "geoframe/all/devices": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": DevicesCallbackSchema,
        },
        "geoframe/all/devicelocationsbyday": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": DeviceLocationsCallbackSchema,
        },
        "geoframe/all/distinctdevicelocations": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": DeviceLocationsCallbackSchema,
        },
        "geoframe/all/locations": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": DeviceLocationsCallbackSchema,
        },
        "geoframe/all/observations": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": ObservationsCallbackSchema,
        },
        "geoframe/common/count": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": CountCallbackSchema,
        },
        "geoframe/common/devices": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": DevicesCallbackSchema,
        },
        "geoframe/demographics/aggregate": {
            "request": GeoJsonDemographicsSchema,
            "response": ResponseBaseSchema,
            "callback": None,  # Unsure of how to deserialize this one
        },
        "geoframe/demographics/aggregate/zipcodes": {
            "request": GeoJsonWithMinMaxSchema,
            "response": ResponseBaseSchema,
            "callback": DemographicZipcodesCallbackSchema,
        },
        "geoframe/extension/devicesinhome": {
            "request": GeoJsonWithMinMaxSchema,
            "response": ResponseBaseSchema,
            "callback": DevicesCallbackSchema,
        },
        "geoframe/extension/devicesinhomecount": {
            "request": GeoJsonWithMinMaxSchema,
            "response": ResponseBaseSchema,
            "callback": CountCallbackSchema,
        },
        "geoframe/household/count": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": CountCallbackSchema,
        },
        "geoframe/household/devices": {
            "request": GeoJsonBaseSchema,
            "response": ResponseBaseSchema,
            "callback": DevicesCallbackSchema,
        },
        "geoframe/legacyextension/devicesinhome": {
            "request": GeoJsonWithMinMaxSchema,
            "response": ResponseBaseSchema,
            "callback": DevicesCallbackSchema,
        },
        "geoframe/political/aggregate": {
            "request": GeoJsonPoliticalAggregateSchema,
            "response": ResponseBaseSchema,
            "callback": None,  # Unsure of how to deserialize this one
        },
        "geoframes/attribution": {
            "request": AttributionSchema,
            "response": ResponseBaseSchema,
            "callback": AttributionCallbackSchema,
        },
    },
    "storage": {
        "geoframe/tradearea": {
            "request": GeoJsonTradesWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/addresses/all/devices": {
            "request": AddressWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/businessaddresses/all/demographics": {
            "request": AddressWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/businessaddresses/all/devices": {
            "request": AddressWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/businessaddresses/all/locations": {
            "request": AddressWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/files/demographics/all": {
            "request": FilesWithMinMaxSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/files/extension/socialnetwork": {
            "request": FilesPoliticalWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/files/political": {
            "request": FilesPoliticalWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframe/all/countgroupedbydevice": {
            "request": GeoJsonWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframe/all/devices": {
            "request": GeoJsonWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframe/all/devicecountataddress": {
            "request": GeoJsonObservationsWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframe/all/devicecountatworkplaceaddress": {
            "request": GeoJsonObservationsWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframe/all/observations": {
            "request": GeoJsonObservationsWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframe/demographics/all": {
            "request": GeoJsonWithMinMaxSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframe/extension/devicesinhome": {
            "request": GeoJsonWithMinMaxSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframe/extension/socialnetwork": {
            "request": GeoJsonSocialsWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframe/legacyextension/devicesinhome": {
            "request": GeoJsonWithMinMaxSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframe/political": {
            "request": GeoJsonPoliticalWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframe/tradearea": {
            "request": GeoJsonTradesWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframes/attribution/devices": {
            "request": AttributionWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframes/attribution/journey": {
            "request": AttributionWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/geoframes/attribution/locations": {
            "request": AttributionWithSaveSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "save/files/extension/devicesinhome": {
            "request": FilesBaseSchema,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
    },
    "publish": {
        "devices/segment/push": {
            "request": DevicesSegment,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
        "geoframe/segment/push": {
            "request": DevicesSegment,
            "response": ResponseWithSaveSchema,
            "callback": CallbackBaseSchema,
        },
    },
}
