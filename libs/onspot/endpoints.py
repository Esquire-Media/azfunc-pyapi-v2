from .schemas import *
from marshmallow import Schema


class Endpoint:
    path: str
    request: Schema = GeoJsonBaseSchema
    response: Schema = ResponseBaseSchema
    callback: Schema = CallbackBaseSchema


class CountDevicesFromGeoframes(Endpoint):
    """Get distinct device count of devices seen within the feature provided."""

    path = "geoframe/all/count"
    callback = CountCallbackSchema


class CountDevicesPerGeoframeFromGeoframes(Endpoint):
    """Get count of devices that were seen in all features provided. Callback should be the same
    value in all features provided"""

    path = "geoframe/common/count"
    callback = CountCallbackSchema


class CountDevicesGroupedByDayFromGeoframes(Endpoint):
    """Get (non distinct) count of device observations by day within the feature provided."""

    path = "geoframe/all/countgroupedbyday"
    request = GeoJsonGroupedByDaySchema
    callback = CountByDayCallbackSchema


class CountDevicesGroupedByDeviceFromGeoframes(Endpoint):
    """Get a count associated with each device that was seen within the feature provided."""

    path = "geoframe/all/countgroupedbydevice"
    callback = CountByDeviceCallbackSchema


class CountDevicesGroupedByHourFromGeoframes(Endpoint):
    """Get (non distinct) count of device observations by hour within the feature provided."""

    path = "geoframe/all/countgroupedbyhour"
    callback = CountByHourCallbackSchema


class CountDevicesGroupedByIntervalFromGeoframes(Endpoint):
    """Get devices grouped by a specific interval."""

    path = "geoframe/all/countgroupedbyinterval"
    request = GeoJsonGroupedByIntervalSchema
    callback = CountByIntervalCallbackSchema


class CountDevicesGroupedByFrequencyFromGeoframes(Endpoint):
    """Get frequency at which we see the same device within the feature, broken down by device
    seen 1, 2, 3, 4-7, 8-15, 16-25, and 26+ times."""

    path = "geoframe/all/devicefrequencycount"
    callback = CountByFrequencyCallbackSchema


class CountHouseholdDevicesFromGeoframes(Endpoint):
    """Get number of devices that belong to households identified by the devices within the
    feature, this does not take into account devices that were not matched to homes."""

    path = "geoframe/extension/devicesinhomecount"
    request = GeoJsonWithMinMaxSchema
    callback = CountCallbackSchema


class CountHouseholdsFromGeoframes(Endpoint):
    """Get number of households that were matched from devices found within this geoframe"""

    path = "geoframe/household/count"
    callback = CountCallbackSchema


class DevicesFromGeoframes(Endpoint):
    """Get each device id that was seen within the feature provided."""

    path = "geoframe/all/devices"
    callback = DevicesCallbackSchema


class DevicesPerGeoframeFromGeoframes(Endpoint):
    """Get devices that were seen in all features provided"""

    path = "geoframe/common/devices"
    callback = DevicesCallbackSchema


class DeviceLocationsFromGeoframes(Endpoint):
    """Get one location per device that was seen within the feature provided."""

    path = "geoframe/all/locations"
    callback = DeviceLocationsCallbackSchema


class DailyDeviceLocationsFromGeoframes(Endpoint):
    """Get one location per device per day that is within the feature provided."""

    path = "geoframe/all/devicelocationsbyday"
    callback = DeviceLocationsCallbackSchema


class DistinctDeviceLocationsFromGeoframes(Endpoint):
    """Get list of distinct locations that devices were seen within the feature provided."""

    path = "geoframe/all/distinctdevicelocations"
    callback = DeviceLocationsCallbackSchema


class ObservationsFromGeoframes(Endpoint):
    """Get all observations within the given feature with (hashed) device ids and timestamp"""

    path = "geoframe/all/observations"
    callback = ObservationsCallbackSchema


class DemographicAggregateFromGeoframes(Endpoint):
    """Get aggregate demographics for devices matched within the feature provided."""

    path = "geoframe/demographics/aggregate"
    request = GeoJsonDemographicsSchema
    callback = None  # Unsure of how to deserialize this one


class DemographicAggregateGroupedByZipcodeFromGeoframes(Endpoint):
    """Get zipcodes of households matched in the feature. This requires a separate end point as
    we don't want to match all members of the household in this case, only one"""

    path = "geoframe/demographics/aggregate/zipcodes"
    request = GeoJsonWithMinMaxSchema
    callback = DemographicZipcodesCallbackSchema


class ResidentialDevicesFromGeoframes(Endpoint):
    """Get device ids that were matched to households identified by the devices within the
    feature provided."""

    path = "geoframe/extension/devicesinhome"
    request = GeoJsonWithMinMaxSchema
    callback = DevicesCallbackSchema


class LegacyResidentialDevicesFromGeoframes(Endpoint):
    """Get active device ids that were matched to households associated to devices observed 1
    year prior to the date range within the feature provided."""

    path = "geoframe/legacyextension/devicesinhome"
    request = GeoJsonWithMinMaxSchema
    callback = DevicesCallbackSchema


class HouseholdDevicesFromGeoframes(Endpoint):
    """Get device ids that were matched to households within this geoframe, this does not
    include additional devices within the household"""

    path = "geoframe/household/devices"
    callback = DevicesCallbackSchema


class DemographicPoliticalAggregateFromGeoframes(Endpoint):
    """Get political aggregate demographics for devices matched within the feature provided."""

    path = "geoframe/political/aggregate"
    request = GeoJsonPoliticalAggregateSchema
    callback = None  # Unsure of how to deserialize this one


class AttributionFromGeoframes(Endpoint):
    """Gets device list from the targetingFeatureCollection, compares this to the
    attributionFeatureCollection and bases the response on the intersection of device ids in
    these 2 sets."""

    path = "geoframes/attribution"
    request = AttributionSchema
    callback = AttributionCallbackSchema


class EndpointToStorage(Endpoint):
    request = GeoJsonWithSaveSchema
    response = ResponseWithSaveSchema


class TradeAreaDevicesFromGeoframesToStorage(EndpointToStorage):
    """Get devices matched to households and workplaces within a specified radius of a geoframe."""

    path = "geoframe/tradearea"
    request = GeoJsonTradesWithSaveSchema


class DevicesFromAddreessesToStorage(EndpointToStorage):
    """Save mobile advertising ids that were found at the addresses provided."""

    path = "save/addresses/all/devices"
    request = AddressWithSaveSchema

class DevicesFromGeoframesToStorage(EndpointToStorage):
    """Save each device id that was seen within the feature provided."""
    
    path = "save/geoframe/all/devices"

from sys import modules

REGISTRY = {
    cls.path: cls
    for class_name in dir(modules[__name__])
    if isinstance((cls := getattr(modules[__name__], class_name)), type)
    and (path := getattr(cls, "path", None))
}
