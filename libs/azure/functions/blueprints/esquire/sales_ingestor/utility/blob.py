import os
import io
from azure.storage.blob import BlobClient
from libs.utils.azure_storage import init_blob_client
import pyarrow.ipc as pa_ipc
import pyarrow as pa

_CONN = os.getenv("SALES_INGEST_CONN_STR")        # put this in App Settings

class _ForwardBlobStream(io.RawIOBase):
    """Forward-only stream → open_stream()."""
    def __init__(self, downloader):
        self._iter = downloader.chunks()        # no args allowed
        self._buf  = b""

    def readable(self): return True

    def readinto(self, b):                      # must fill *exactly* len(b) or 0
        need = len(b)
        while len(self._buf) < need:
            try:
                self._buf += next(self._iter)
            except StopIteration:
                break
        chunk, self._buf = self._buf[:need], self._buf[need:]
        b[:len(chunk)] = chunk
        return len(chunk)

class _RandomAccessBlob(io.RawIOBase):
    """Minimal random-access wrapper → open_file()."""
    def __init__(self, blob: BlobClient, chunk_size=8<<20):
        self._blob, self._pos, self._size = blob, 0, blob.get_blob_properties().size
        self._chunk = chunk_size

    # -- Python file-object protocol ------------------------------------------------
    def readable(self): return True
    def seekable(self): return True
    def tell(self):     return self._pos

    def seek(self, offset, whence=io.SEEK_SET):
        if   whence == io.SEEK_CUR: offset += self._pos
        elif whence == io.SEEK_END: offset  = self._size + offset
        if offset < 0: raise ValueError("negative seek")
        self._pos = offset
        return self._pos

    def read(self, n=-1):
        if n < 0 or self._pos + n > self._size:
            n = self._size - self._pos           # to EOF
        # chunk to keep memory bounded
        data, remaining = bytearray(), n
        while remaining:
            take = min(remaining, self._chunk)
            part = self._blob.download_blob(
                offset=self._pos, length=take
            ).readall()
            if not part:
                break
            data.extend(part)
            self._pos     += len(part)
            remaining     -= len(part)
        return bytes(data)
    
_MAGIC = b"ARROW1"

def _arrow_reader(blob, chunk_size: int = 8 << 20) -> pa.RecordBatchReader:
    """Return the correct reader for an Arrow *file* or *stream* blob."""
    MAGIC = b"ARROW1"
    head = blob.download_blob(offset=0, length=len(MAGIC)).readall()

    if head == MAGIC:                               # file format
        return pa_ipc.open_file(_RandomAccessBlob(blob, chunk_size))
    else:                                           # streaming format
        return pa_ipc.open_stream(
            _ForwardBlobStream(blob.download_blob())
        )