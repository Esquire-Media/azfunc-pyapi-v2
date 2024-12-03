import os
import numpy as np
import pandas as pd
from azure.storage.blob.aio import ContainerClient
from io import BytesIO
import asyncio


async def get_all_neighbors_async(address_df, N, same_side_only=True, limit=-1):
    """
    Main function to get address neighbors by batching by the CSZ for quicker reading and using async calls.

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
    tasks = [
        process_city_group(city, state, zip_code, part_df, N, same_side_only)
        for (city, state, zip_code), part_df in address_df.groupby(["city", "state", "zip_code"])
        ]

    # Run tasks
    results_list = await asyncio.gather(*tasks)

    # return things if there are any, optionally heading
    final_result = pd.concat(results_list, ignore_index=True) if results_list else pd.DataFrame()
    return final_result.head(limit) if limit > 0 else final_result

async def process_city_group(city, state, zip_code, part_df, N, same_side_only):
    """
    Process a group of addresses for a specific city, state, and zip code 
    to find neighboring addresses.

    Parameters
    ----------
    city : str
        The name of the city to process.
    state : str
        The state associated with the city.
    zip_code : str
        The zip code for the region being processed.
    part_df : pandas.DataFrame
        The subset of the address DataFrame corresponding to this group.
        Must contain columns: "street_name", "street_number".
    N : int
        Number of neighbors to find in each direction (if possible).
    same_side_only : bool
        If True, find neighbors only on the same side of the street (even/odd logic).

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the neighboring addresses for the input group.
        Returns an empty DataFrame if no neighbors are found or if data loading fails.

    """
    city = city.replace(" ", "_")
    try:
        estated_data = await load_estated_data_partitioned_blob_async(
            f"estated_partition_testing/state={state}/zip_code={zip_code}/city={city}/"
        )
    except Exception:
        return pd.DataFrame()

    results = []
    for street_name, street_addresses in part_df.groupby("street_name"):
        if estated_data.empty:
            continue
        street_data = estated_data[
            estated_data["street_name"] == str(street_name).upper()
        ]
        if not street_data.empty:
            neighbors = find_neighbors_for_street(
                street_data, street_addresses, N, same_side_only
            )
            if not neighbors.empty:
                results.append(neighbors)

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()

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

    addresses = addresses.dropna(subset=["street_number"]).copy()
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
        base_evenness = addresses["base_street_num"] % 2
        evenness_map = dict(zip(addresses.index, base_evenness))
        neighbors["base_evenness"] = neighbors["base_address_id"].map(evenness_map)
        neighbors = neighbors[
            neighbors["street_number"] % 2 == neighbors["base_evenness"]
        ]

    return neighbors.drop(
        columns=["base_evenness", "base_address_id", "neighbor_index"]
    ).reset_index(drop=True)

async def load_parquet_from_blob_async(blob_dir_path):
    """
    Load parquet files asynchronously from Azure Blob Storage directory.

    Parameters
    ----------
    blob_dir_path : str
        The directory path in Azure Blob Storage.

    Returns
    -------
    pandas.DataFrame
        Concatenated DataFrame of all parquet files.
    """
    async with ContainerClient.from_connection_string(
        os.environ["DATALAKE_CONN_STR"], container_name="general"
    ) as container_client:
        # Use async for to iterate blobs
        blobs = []
        async for blob in container_client.list_blobs(name_starts_with=blob_dir_path):
            blobs.append(blob)

        if not blobs:
            return pd.DataFrame()

        async def load_blob(blob):
            if blob.size == 0:
                return pd.DataFrame()

            blob_client = container_client.get_blob_client(blob)
            blob_stream = await blob_client.download_blob()
            return pd.read_parquet(
                BytesIO(await blob_stream.readall()),
                columns=["street_number", "street_name", "city", "state", "zip_code"]
                )

        tasks = [load_blob(blob) for blob in blobs]
        dataframes = await asyncio.gather(*tasks)

        non_empty_dataframes = [df for df in dataframes if not df.empty]
        if non_empty_dataframes:
            final_df = (
                pd.concat(non_empty_dataframes, ignore_index=True)
                .drop_duplicates()
                .dropna(subset=["street_number"])
            )
            return final_df
        else:
            return pd.DataFrame()


async def load_estated_data_partitioned_blob_async(table_path):
    """
    Async version of loading estated data from a given blob storage path.

    Parameters
    ----------
    table_path : str
        The path to the partitioned blob storage directory.

    Returns
    -------
    pandas.DataFrame
        The formatted estated data.
    """
    try:
        blob_df = await load_parquet_from_blob_async(table_path)
    except Exception as e:
        return pd.DataFrame()

    if blob_df.empty:
        return pd.DataFrame()

    # Process DataFrame
    blob_df = blob_df.dropna(subset=["street_number", "street_name"], how="any")

    blob_df["street_number"] = pd.to_numeric(
        blob_df["street_number"], errors="coerce"
    ).astype("Int64")
    blob_df["street_name"] = blob_df["street_name"].astype("str")

    return blob_df