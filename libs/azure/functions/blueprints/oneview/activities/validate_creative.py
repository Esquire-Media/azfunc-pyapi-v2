# File: libs/azure/functions/blueprints/async_tasks/activities/validate_creative.py

from azure.durable_functions import DurableOrchestrationClient, EntityId
from libs.azure.functions.blueprints.oneview.schemas import RequestSchema
from libs.azure.functions import Blueprint
import isobmff
import fsspec
import hashlib


bp = Blueprint()


# Define an Azure Durable Activity Function
@bp.activity_trigger(input_name="instanceId")
@bp.durable_client_input(client_name="client")
async def oneview_activity_validate_creative(
    instanceId: str, client: DurableOrchestrationClient
) -> str:
    """
    Validate the creative video file with certain conditions.

    This function reads the state of a specific entity, tries to open the file located at a URL,
    validates it with certain conditions, and then generates an MD5 hash of the 'moov' box in the ISOBMFF file.
    After that, it sends a signal to an entity in an Azure Durable Task Framework task hub and returns the MD5 hash.

    Parameters
    ----------
    instanceId : str
        The unique identifier of the entity instance.
    client : DurableOrchestrationClient
        The Azure Durable Task Framework client.

    Returns
    -------
    str
        The MD5 hash of the 'moov' box in the ISOBMFF file.

    Raises
    ------
    Exception
        If the file cannot be read or the video file does not meet the necessary conditions.
    """

    # Get state
    state = await client.read_entity_state(
        EntityId("oneview_entity_request", instanceId)
    )
    while not state.entity_exists:
        state = await client.read_entity_state(
            EntityId("oneview_entity_request", instanceId)
        )
    state = RequestSchema().loads(state.entity_state)

    # Create a filesystem object for HTTP
    file_system = fsspec.filesystem("http")
    try:
        # Open the file from the provided URL
        file = file_system.open(
            state["request"]["creative"], mode="rb", block_size=1024 * 64
        )
    except:
        # Raise an exception if the file cannot be read
        raise Exception(f'Could not read data from {state["request"]["creative"]}')

    # Create an ISO Base Media File Format (ISOBMFF) scanner object
    iso = isobmff.Scanner(file)

    # Check if the file has exactly two media tracks
    tracks = [t for t in iso["moov"]["trak"]]
    if len(tracks) != 2:
        raise Exception(
            "There must only be two media tracks (one video track and one sound track)."
        )

    # Iterate over the media tracks
    for track in tracks:
        # Check if the track is a video track
        if track["mdia"]["hdlr"].handler_type == "vide":
            # Check the resolution of the video track
            ratio = track["tkhd"].width / track["tkhd"].height
            if ratio != 16 / 9 and ratio != 4 / 3:
                raise Exception(
                    "The resolution ratio of the video track is not 16:9 or 4:3."
                )

            # Check the width and height of the video track
            if track["tkhd"].width > 1920:
                raise Exception(
                    "The width of the video track is greater than 1920 pixels."
                )
            if track["tkhd"].height > 1080:
                raise Exception(
                    "The height of the video track is greater than 1080 pixels."
                )
            if track["tkhd"].width < 400:
                raise Exception("The width of the video track is less than 400 pixels.")
            if track["tkhd"].height < 300:
                raise Exception(
                    "The height of the video track is less than 300 pixels."
                )

            # Check the duration of the video track
            duration = track["mdia"]["mdhd"].duration / track["mdia"]["mdhd"].timescale
            if (
                duration < 14.5
                or 15.5 < duration < 29.5
                or 30.5 < duration < 59.5
                or 60.5 < duration
            ):
                raise Exception(
                    "The video track must have a duration of 15, 30, or 60 seconds."
                )

            # Check the frame rate of the video track
            frames = track["mdia"]["minf"]["stbl"]["stsz"].sample_count
            fps = frames / duration
            if fps != 29.97 and fps != 25 and fps != 23.98 and fps != 30:
                raise Exception(
                    "The video track must have a frame rate of 29.97, 25, or 23.98. A frame rate of 29.97 is recommended."
                )

            # Check the codec of the video track
            if [
                codec for codec in track["mdia"]["minf"]["stbl"]["stsd"].sample_entries
            ][0].type != "avc1":
                raise Exception("The video track must use H.264 (AVC1) compression.")

        # Check if the track is an audio track
        if track["mdia"]["hdlr"].handler_type == "soun":
            # Check the sample rate
            sample_rate = track["mdia"]["mdhd"].timescale
            if sample_rate != 44100 and sample_rate != 48000:
                raise Exception(
                    "The sample rate for the sound tack must be 44.1KHz or 48KHz."
                )

            # Check the duration of the audio track
            duration = track["mdia"]["mdhd"].duration / sample_rate
            if (
                duration < 14.5
                or 15.5 < duration < 29.5
                or 30.5 < duration < 59.5
                or 60.5 < duration
            ):
                raise Exception("The audio track must be 15, 30, or 60 seconds long.")

            # Check the codec of the audio track
            if [
                codec for codec in track["mdia"]["minf"]["stbl"]["stsd"].sample_entries
            ][0].type != "mp4a":
                raise Exception("The sound track must use AAC (MP4A) compression.")

    # Generate the MD5 hash of the 'moov' box in the ISOBMFF file
    md5 = hashlib.md5(iso["moov"].slice.read()).hexdigest()

    await client.signal_entity(
        EntityId("oneview_entity_request", instanceId), "creative_md5", md5
    )

    return md5
