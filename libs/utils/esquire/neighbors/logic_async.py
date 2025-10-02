import numpy as np
import pandas as pd
from azure.storage.blob.aio import ContainerClient
from io import BytesIO
import asyncio, os

async def get_all_neighbors(
    address_df: pd.DataFrame, N: int, same_side_only: bool = True, limit: int = -1
):
    """
    Main function to get address neighbors by batching by the CSZ for quicker reading.

    Parameters
    ----------
    address_df : pandas.DataFrame
        Formatted query street data from smarty. Must include headers street_number, street_name, city, state, zip_code.
    N : int
        Number of neighbors to fetch in each direction (if possible).
    same_side_only : bool, optional
        Flag to indicate if we're querying both or one side of the street, by default True.
    limit : int, optional
        Optional maximum number of records to return, by default -1.

    Returns
    -------
    pandas.DataFrame
        All neighbors of the input addresses.
    """
    async def process_group(city, state, zip_code, part_df):
        city = city.replace(" ", "_")
        try:
            estated_data = await load_estated_data_partitioned_blob(
                f"estated_partition_testing/state={state}/zip_code={zip_code}/city={city}/"
            )
        except Exception as e:
            return None

        group_results = []
        for street_name, street_addresses in part_df.groupby("street_name"):
            street_data = estated_data[
                estated_data["street_name"] == str(street_name).upper()
            ]
            if street_data.empty:
                continue

            neighbors = find_neighbors_for_street(
                street_data, street_addresses, N, same_side_only
            )
            if not neighbors.empty:
                group_results.append(neighbors)

        if group_results:
            return pd.concat(group_results, ignore_index=True)
        else:
            return None

    # Group data by city, state, and zip_code to batch operations
    tasks = (
        process_group(city, state, zip_code, part_df)
        for (city, state, zip_code), part_df in address_df.groupby(["city", "state", "zip_code"])
    )

    # Using asyncio.as_completed to reduce memory consumption while processing results
    results_list = []
    for coro in asyncio.as_completed(tasks):
        result = await coro
        if result is not None:
            results_list.append(result)
    
    # Concatenate results
    if results_list:
        return pd.concat(results_list, ignore_index=True).head(limit) if limit > 0 else pd.concat(results_list, ignore_index=True)
    else:
        return pd.DataFrame()

def find_neighbors_for_street(
    data: pd.DataFrame, addresses: pd.DataFrame, N: int, same_side_only: bool
) -> pd.DataFrame:
    """
    Vectorized way of finding neighbors for all addresses on a given street.

    Parameters
    ----------
    data : pandas.DataFrame
        The relevant estated data on this street.
    addresses : pandas.DataFrame
        The input addresses on this street.
    N : int
        Number of neighbors to fetch in each direction (if possible).
    same_side_only : bool
        Flag to indicate if we're querying both or one side of the street.

    Returns
    -------
    pandas.DataFrame
        All neighbors of the input addresses.
    """
    data = (
        data.sort_values(by="street_number")
        .dropna(subset=["street_number"])
        .reset_index(drop=True)
    )
    increment = 2 if same_side_only else 1

    addresses = addresses.dropna(subset=["street_number"])
    addresses["base_street_num"] = pd.to_numeric(
        addresses["street_number"], errors="coerce"
    )
    addresses = addresses.dropna(subset=["base_street_num"])

    if data.empty or addresses.empty:
        return pd.DataFrame()

    start_indices = data["street_number"].searchsorted(
        addresses["base_street_num"] - N * increment, side="left"
    )
    end_indices = data["street_number"].searchsorted(
        addresses["base_street_num"] + N * increment, side="right"
    )

    if len(start_indices) == 0 or len(end_indices) == 0:
        return pd.DataFrame()

    try:
        base_ids = np.repeat(addresses.index, end_indices - start_indices)
        neighbor_indices = np.concatenate(
            [np.arange(start, end) for start, end in zip(start_indices, end_indices)]
        )
    except ValueError:
        return pd.DataFrame()

    result = pd.DataFrame(
        {"base_address_id": base_ids, "neighbor_index": neighbor_indices}
    )
    neighbors = result.merge(data, left_on="neighbor_index", right_index=True)

    if same_side_only:
        evenness_map = addresses["base_street_num"] % 2
        neighbors["base_evenness"] = neighbors["base_address_id"].map(evenness_map)
        neighbors = neighbors[
            neighbors["street_number"] % 2 == neighbors["base_evenness"]
        ]

    return neighbors.drop(
        columns=["base_evenness", "base_address_id", "neighbor_index"], errors="ignore"
    ).reset_index(drop=True)


async def load_parquet_from_blob(container_client: ContainerClient, blob_dir_path):
    """
    Load parquet files from an Azure Blob Storage directory.

    Parameters
    ----------
    blob_dir_path : str
        The directory path in the Azure Blob Storage container.

    Returns
    -------
    pandas.DataFrame
        The concatenated DataFrame of all parquet files in the given directory.
    """
    cols = ["street_number", "street_name", "formatted_street_address", "city", "state", "zip_code", "zip_plus_four_code"]
    dfs = []

    async for blob in container_client.list_blobs(name_starts_with=blob_dir_path):
        if getattr(blob, "size", 1) == 0:
            continue

        # BlobClient supports async context manager – this ensures its aiohttp session is closed.
        async with container_client.get_blob_client(blob=blob.name) as blob_client: 
            downloader = await blob_client.download_blob()
            try:
                data = await downloader.readall()
            finally:
                # Some SDK versions require explicit close() on the downloader to release the connection.
                if hasattr(downloader, "close"):
                    await downloader.close()

        df = pd.read_parquet(BytesIO(data), columns=cols)
        if not df.empty:
            df = df.dropna(subset=["street_number", "street_name"], how="any")
            if not df.empty:
                dfs.append(df)

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=cols)


async def load_estated_data_partitioned_blob(table_path):
    """
    Read in estated data for a given path, ensuring that the street number is an integer.

    Parameters
    ----------
    table_path : str
        The path to the partitioned blob storage directory.

    Returns
    -------
    pandas.DataFrame
        The formatted estated data.
    """    
    conn_str = os.environ["DATALAKE_CONN_STR"]
    async with ContainerClient.from_connection_string(conn_str, container_name="general") as container:
        blob_df = await load_parquet_from_blob(container, table_path)
        # rename for the onspot orchestrator
        blob_df = blob_df.rename(columns={
            'formatted_street_address':'address',
            'zip_code':'zipCode',
            'zip_plus_four_code':'plus4Code'
            })

    if not blob_df.empty:
        blob_df = blob_df.drop_duplicates()
        blob_df["street_number"] = pd.to_numeric(blob_df["street_number"], errors="coerce").astype("Int64")
        blob_df["street_name"] = blob_df["street_name"].astype("str")
    return blob_df
