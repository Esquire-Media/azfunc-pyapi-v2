from libs.azure.functions import Blueprint
import io
from pymp4.parser import Box
import requests
import struct


bp = Blueprint()


# Activity
@bp.activity_trigger(input_name="url")
def validate_creative(url: str):
    response = requests.get(url, stream=True)

    box_data = b''
    buffer_size = 1024 * 1024  # 1MB
    header_data = b''
    finished = False

    while not finished:
        buffer = next(response.iter_content(buffer_size))
        box_data += buffer

        while len(box_data) >= 8:
            box_size, box_type = struct.unpack('>I4s', box_data[:8])
            box_type = box_type.decode()

            if len(box_data) >= box_size:
                # We have the whole box, so add it to the header data
                header_data += box_data[:box_size]
                box_data = box_data[box_size:]

                # If we've found the 'moov' box, stop
                if box_type == 'moov':
                    finished = True
                    break
            else:
                # We don't have the whole box, so wait for more data
                break

    header = Box.parse(io.BytesIO(header_data))
    return header