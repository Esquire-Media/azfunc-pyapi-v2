import pytz
from timezonefinder import TimezoneFinder
from datetime import datetime as dt


def local_time_to_utc(local_time: dt, local_timezone: pytz.timezone) -> dt:
    """
    Given a local time and the timezone associated with it, return that time in UTC.
    """
    # initialize the timezone objects
    zoned_local_time = local_timezone.localize(local_time)
    utc_time = zoned_local_time.astimezone(pytz.utc)

    return utc_time


def get_local_timezone(latitude: float, longitude: float) -> pytz.timezone:
    """
    Given a latlong, return the pytz timezone of that point.
    """
    # initialize the timezone objects
    tf = TimezoneFinder()
    # returns the closest reasonable candidate, even if the point is not strictly in a timezone polygon

    # get end time in local timezone
    local_timezone = pytz.timezone(tf.timezone_at(lat=latitude, lng=longitude))
    return local_timezone
