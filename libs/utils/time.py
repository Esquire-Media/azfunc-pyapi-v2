import pytz
from tzwhere import tzwhere
from datetime import datetime as dt

def local_time_to_utc(local_time:dt, local_timezone:pytz.timezone) -> dt:
    """
    Given a local time and the timezone associated with it, return that time in UTC.
    """
    # initialize the timezone objects
    zoned_local_time = local_timezone.localize(local_time)
    utc_time = zoned_local_time.astimezone(pytz.utc)

    return utc_time

def get_local_timezone(lat:float, lon:float) -> pytz.timezone:
    """
    Given a latlong, return the pytz timezone of that point.
    """
    # initialize the timezone objects
    tzw = tzwhere.tzwhere(forceTZ=True)
    # forceTZ will find the closest reasonable candidate if the point is not strictly in a timezone polygon
    # this comes up sometimes in coastal areas on the country border

    # get end time in local timezone
    local_timezone = pytz.timezone(tzw.tzNameAt(latitude=lat, longitude=lon, forceTZ=True))
    return local_timezone