from azure.durable_functions import Blueprint
from azure.storage.blob import BlobClient
from datetime import datetime as dt, timedelta, timezone
from libs.utils.azure_storage import get_blob_sas
from libs.utils.time import get_local_timezone, local_time_to_utc
import orjson as json, os, pandas as pd

# Create a Blueprint instance for defining Azure Functions
bp = Blueprint()


def _as_utc(value: dt) -> dt:
    """Normalize datetimes returned by the timezone utility to aware UTC."""
    return (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )


def _format_onspot_utc(value: dt) -> str:
    """Onspot expects UTC ISO timestamps without a timezone suffix."""
    return _as_utc(value).replace(tzinfo=None).isoformat(timespec="seconds")


# Define an activity function
@bp.activity_trigger(input_name="settings")
def activity_locationInsights_createObservationsRequest(settings: dict):
    
    # Build the requested interval in the location's local calendar.
    start_date_local = dt.fromisoformat(settings["endDate"]) - timedelta(days=111)
    requested_end_local = (
        dt.fromisoformat(settings["endDate"])
        + timedelta(days=8)
        - timedelta(seconds=1)
    )
    # Fixed at the start of the root orchestration (not activity wall-clock time).
    today_utc = _as_utc(dt.fromisoformat(settings["today"].replace("Z", "+00:00")))

    # load the locations blob into Pandas using SAS
    blob_client = BlobClient.from_connection_string(
        conn_str=os.environ[settings["runtime_container"]["conn_str"]],
        container_name=settings["runtime_container"]["container_name"],
        blob_name=settings["runtime_container"]["location_blob"],
    )
    locations = pd.read_csv(get_blob_sas(blob=blob_client))
    loc = locations.iloc[0]

    # build a feature collection with the fields required for Onspot observations request

    # determine the timezone where this location exists
    local_timezone = get_local_timezone(
        latitude=loc["Latitude"], longitude=loc["Longitude"]
    )

    # Apply the rolling 48-hour restriction *after* converting to UTC.
    start_utc = _as_utc(
        local_time_to_utc(local_time=start_date_local, local_timezone=local_timezone)
    )
    requested_end_utc = _as_utc(
        local_time_to_utc(local_time=requested_end_local, local_timezone=local_timezone)
    )
    # One extra second avoids a boundary that Onspot may treat as "within" 2 days.
    safe_cutoff_utc = today_utc - timedelta(days=2, seconds=1)
    end_utc = min(requested_end_utc, safe_cutoff_utc)

    if end_utc <= start_utc:
        raise ValueError("Observation date range ends before the safe Onspot cutoff")

    # build the feature payload
    feature_collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": json.loads(loc["Geometry"]),
                "properties": {
                    "hash": False,
                    "fileFormat": {"delimiter": ",", "quoteEncapsulate": True},
                    "name": f"{loc['ESQ_ID']}",
                    "fileName": f"{loc['ESQ_ID']}",
                    "start": _format_onspot_utc(start_utc),
                    "end": _format_onspot_utc(end_utc),
                },
            }
        ],
    }

    return feature_collection