import asyncio
import os
import weakref
from contextlib import asynccontextmanager
from dataclasses import dataclass
from io import BytesIO
from typing import Any, AsyncIterator, Coroutine, Iterable, Optional, TypeVar

import numpy as np
import pandas as pd
from azure.storage.blob.aio import ContainerClient

from libs.utils.azure_storage import _create_transport


_PARQUET_COLS: tuple[str, ...] = (
    "street_number",
    "street_name",
    "formatted_street_address",
    "city",
    "state",
    "zip_code",
    "zip_plus_four_code",
)

_DEFAULT_PARTITION_CONCURRENCY = int(os.getenv("ESTATED_PARTITION_LOAD_CONCURRENCY", "4"))
_DEFAULT_DOWNLOAD_CONCURRENCY = int(os.getenv("ESTATED_BLOB_DOWNLOAD_CONCURRENCY", "8"))
_DEFAULT_DECODE_CONCURRENCY = int(os.getenv("ESTATED_PARQUET_DECODE_CONCURRENCY", "4"))
_DEFAULT_GROUP_CONCURRENCY = int(os.getenv("NEIGHBOR_GROUP_PROCESS_CONCURRENCY", "8"))

T = TypeVar("T")


@dataclass(frozen=True)
class _Semaphores:
    partition_load: asyncio.Semaphore
    blob_download: asyncio.Semaphore
    parquet_decode: asyncio.Semaphore
    group_process: asyncio.Semaphore


_LOOP_SEMS: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, _Semaphores]" = (
    weakref.WeakKeyDictionary()
)


def _get_semaphores() -> _Semaphores:
    loop = asyncio.get_running_loop()
    sems = _LOOP_SEMS.get(loop)
    if sems is None:
        sems = _Semaphores(
            partition_load=asyncio.Semaphore(_DEFAULT_PARTITION_CONCURRENCY),
            blob_download=asyncio.Semaphore(_DEFAULT_DOWNLOAD_CONCURRENCY),
            parquet_decode=asyncio.Semaphore(_DEFAULT_DECODE_CONCURRENCY),
            group_process=asyncio.Semaphore(_DEFAULT_GROUP_CONCURRENCY),
        )
        _LOOP_SEMS[loop] = sems
    return sems


@asynccontextmanager
async def _acquire(sem: asyncio.Semaphore) -> AsyncIterator[None]:
    await sem.acquire()
    try:
        yield
    finally:
        sem.release()


async def _bounded_as_completed(
    coros: Iterable[Coroutine[Any, Any, T]],
    limit: int,
) -> AsyncIterator[T]:
    """
    Consume an iterable of coroutine objects with bounded in-flight concurrency.

    Unlike asyncio.as_completed(), this does NOT eagerly schedule everything at once.
    """
    if limit <= 0:
        limit = 10_000_000

    it = iter(coros)
    pending: set[asyncio.Task[T]] = set()

    for _ in range(limit):
        try:
            pending.add(asyncio.create_task(next(it)))
        except StopIteration:
            break

    while pending:
        done, pending = await asyncio.wait(
            pending, return_when=asyncio.FIRST_COMPLETED
        )

        for task in done:
            yield task.result()

        for _ in range(len(done)):
            try:
                pending.add(asyncio.create_task(next(it)))
            except StopIteration:
                break


async def get_all_neighbors(
    address_df: pd.DataFrame, N: int, same_side_only: bool = True, limit: int = -1
) -> pd.DataFrame:
    """
    Main function to get address neighbors by batching by the CSZ for quicker reading.
    """
    _ = _get_semaphores()  # semaphores are used by the loaders; kept here for clarity

    async def process_group(
        city: str, state: str, zip_code: str, part_df: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        # safe_city = city.replace(" ", "_")
        try:
            # estated_data = await load_estated_data_partitioned_blob(
            #     f"estated_partition_testing/state={state}/zip_code={zip_code}/city={safe_city}/"
            # )
            estated_data = await load_estated_data_db(
                city=city,
                state=state,
                zip_code=zip_code,
                bind='keystone'
            )
        except Exception:
            return None

        if estated_data.empty:
            return None

        group_results: list[pd.DataFrame] = []
        estated_data_street = estated_data["street_name"]

        for street_name, street_addresses in part_df.groupby("street_name"):
            street_data = estated_data[estated_data_street == str(street_name).upper()]
            if street_data.empty:
                continue

            neighbors = find_neighbors_for_street(
                street_data, street_addresses, N, same_side_only
            )
            if not neighbors.empty:
                group_results.append(neighbors)

        if not group_results:
            return None
        return pd.concat(group_results, ignore_index=True)

    group_coros: Iterable[Coroutine[Any, Any, Optional[pd.DataFrame]]] = (
        process_group(city, state, zip_code, part_df)
        for (city, state, zip_code), part_df in address_df.groupby(
            ["city", "state", "zip_code"]
        )
    )

    results_list: list[pd.DataFrame] = []
    async for result in _bounded_as_completed(group_coros, limit=_DEFAULT_GROUP_CONCURRENCY):
        if result is not None and not result.empty:
            results_list.append(result)

    if not results_list:
        return pd.DataFrame()

    out = pd.concat(results_list, ignore_index=True)
    if limit > 0:
        out = out.head(limit)
    return out


def find_neighbors_for_street(
    data: pd.DataFrame, addresses: pd.DataFrame, N: int, same_side_only: bool
) -> pd.DataFrame:
    """
    Vectorized way of finding neighbors for all addresses on a given street.
    """
    data = (
        data.sort_values(by="street_number")
        .dropna(subset=["street_number"])
        .reset_index(drop=True)
    )
    _ = 2 if same_side_only else 1  # increment (parity filtering is applied below)

    addresses = addresses.dropna(subset=["street_number"]).copy()
    addresses["base_street_num"] = pd.to_numeric(
        addresses["street_number"], errors="coerce"
    )
    addresses = addresses.dropna(subset=["base_street_num"])

    if data.empty or addresses.empty:
        return pd.DataFrame()

    idx_map = pd.Series(data.index, index=data["street_number"])

    start_indices: list[int] = []
    end_indices: list[int] = []
    for base_num in addresses["base_street_num"]:
        base_idx = idx_map.searchsorted(base_num)
        start_indices.append(max(0, int(base_idx) - N))
        end_indices.append(min(len(data), int(base_idx) + N))

    start_indices_arr = np.asarray(start_indices, dtype=np.int64)
    end_indices_arr = np.asarray(end_indices, dtype=np.int64)

    if start_indices_arr.size == 0 or end_indices_arr.size == 0:
        return pd.DataFrame()

    try:
        base_ids = np.repeat(
            addresses.index.to_numpy(),
            end_indices_arr - start_indices_arr,
        )
        neighbor_indices = np.concatenate(
            [np.arange(start, end) for start, end in zip(start_indices_arr, end_indices_arr)]
        )
    except ValueError:
        return pd.DataFrame()

    result = pd.DataFrame(
        {"base_address_id": base_ids, "neighbor_index": neighbor_indices}
    )
    neighbors = result.merge(data, left_on="neighbor_index", right_index=True)

    if same_side_only:
        evenness_map = (addresses["base_street_num"] % 2).to_dict()
        neighbors["base_evenness"] = neighbors["base_address_id"].map(evenness_map)
        neighbors = neighbors[
            (neighbors["street_number"] % 2) == neighbors["base_evenness"]
        ]

    return (
        neighbors.drop(
            columns=["base_evenness", "base_address_id", "neighbor_index"], errors="ignore"
        )
        .reset_index(drop=True)
    )


async def load_parquet_from_blob(
    container_client: ContainerClient,
    blob_dir_path: str,
    *,
    download_semaphore: Optional[asyncio.Semaphore] = None,
    decode_semaphore: Optional[asyncio.Semaphore] = None,
) -> pd.DataFrame:
    """
    Load parquet files from an Azure Blob Storage directory, with bounded concurrency.
    """
    sems = _get_semaphores()
    download_sem = download_semaphore or sems.blob_download
    decode_sem = decode_semaphore or sems.parquet_decode

    dfs: list[pd.DataFrame] = []

    async for blob in container_client.list_blobs(name_starts_with=blob_dir_path):
        if getattr(blob, "size", 1) == 0:
            continue

        async with _acquire(download_sem):
            async with container_client.get_blob_client(blob=blob.name) as blob_client:
                downloader = await blob_client.download_blob()
                try:
                    data = await downloader.readall()
                finally:
                    close_fn = getattr(downloader, "close", None)
                    if close_fn is not None:
                        maybe_awaitable = close_fn()
                        if asyncio.iscoroutine(maybe_awaitable):
                            await maybe_awaitable

        async with _acquire(decode_sem):
            df = await asyncio.to_thread(
                pd.read_parquet, BytesIO(data), columns=list(_PARQUET_COLS)
            )

        if df.empty:
            continue

        df = df.dropna(subset=["street_number", "street_name"], how="any")
        if not df.empty:
            dfs.append(df)

    if not dfs:
        return pd.DataFrame(columns=list(_PARQUET_COLS))
    return pd.concat(dfs, ignore_index=True)


async def load_estated_data_partitioned_blob(
    table_path: str,
    *,
    partition_semaphore: Optional[asyncio.Semaphore] = None,
    download_semaphore: Optional[asyncio.Semaphore] = None,
    decode_semaphore: Optional[asyncio.Semaphore] = None,
) -> pd.DataFrame:
    """
    Read in estated data for a given path, ensuring that the street number is an integer.
    Uses a semaphore to cap concurrent partition loads so the function node isn't overwhelmed.
    """
    sems = _get_semaphores()
    partition_sem = partition_semaphore or sems.partition_load

    conn_str = os.environ["DATALAKE_CONN_STR"]

    async with _acquire(partition_sem):
        async with ContainerClient.from_connection_string(
            conn_str, container_name="general"
        ) as container:
            blob_df = await load_parquet_from_blob(
                container,
                table_path,
                download_semaphore=download_semaphore,
                decode_semaphore=decode_semaphore,
            )
            blob_df = blob_df.rename(
                columns={
                    "formatted_street_address": "address",
                    "zip_code": "zipCode",
                    "zip_plus_four_code": "plus4Code",
                }
            )

    if blob_df.empty:
        return blob_df

    blob_df = blob_df.drop_duplicates()
    blob_df["street_number"] = pd.to_numeric(
        blob_df["street_number"], errors="coerce"
    ).astype("Int64")
    blob_df["street_name"] = blob_df["street_name"].astype("str")
    return blob_df

import pandas as pd
from sqlalchemy import text
from libs.data import from_bind

def load_estated_data_db(
    *,
    city: str,
    state: str,
    zip_code: str,
    bind: str = "keystone",
) -> pd.DataFrame:
    """
    Drop-in replacement for blob estated loader.
    Loads estated rows for a single CSZ partition from Postgres.
    """

    provider = from_bind(bind)
    session = provider.connect()

    try:
        conn = session.connection()
        raw = getattr(conn, "connection", None)
        cur = raw.cursor()

        cur.execute(
            """
            SELECT
                street_number,
                street_name,
                address,
                city,
                state,
                "zipCode",
                "plus4Code"
            FROM utils.estated
            WHERE city = %s
              AND state = %s
              AND "zipCode" = %s
              AND street_number IS NOT NULL
            """,
            (city, state, zip_code),
        )

        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()

        cols = [d[0] for d in cur.description]
        df = pd.DataFrame(rows, columns=cols)

        df["street_number"] = pd.to_numeric(
            df["street_number"], errors="coerce"
        ).astype("Int64")

        df["street_name"] = df["street_name"].astype(str)

        return df.drop_duplicates()

    finally:
        session.close()
