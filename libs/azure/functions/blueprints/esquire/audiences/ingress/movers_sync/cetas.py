# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/cetas.py

def create_cetas_query(blob_type:list):
    """
    Generate a query to build a new CETAS statement.
    """

    # This CETAS query also dedupes before building
    # NOTE: Partition excludes secondary_designator, as we want "Unit 1" and "Apt #1" to count as the same address
    return f"""
        -- Main SELECT statement to retrieve the desired columns
        SELECT
            [date],
            [address],
            [primaryNumber],
            [streetPredirection],
            [streetName],
            [streetSuffix],
            [streetPostdirection],
            [secondaryDesignator],
            [secondaryNumber],
            [city],
            [state],
            [zipcode],
            [plus4Code],
            [carrierCode],
            [latitude],
            [longitude],
            [homeOwnership],
            [addressType],
            [estimatedIncome],
            [estimatedHomeValue],
            [estimatedAge],
            [h3_index]
        FROM (
            -- Subquery to apply ROW_NUMBER() function for deduplication based on certain criteria
            SELECT
                [date],
                [address],
                [primaryNumber],
                [streetPredirection],
                [streetName],
                [streetSuffix],
                [streetPostdirection],
                [secondaryDesignator],
                [secondaryNumber],
                [city],
                [state],
                [zipcode],
                [plus4Code],
                [carrierCode],
                [latitude],
                [longitude],
                [homeOwnership],
                [addressType],
                [estimatedIncome],
                [estimatedHomeValue],
                [estimatedAge],
                [h3_index],
                ROW_NUMBER() OVER (
                    PARTITION BY 
                        [primaryNumber], 
                        [streetPredirection], 
                        [streetName], 
                        [streetSuffix], 
                        [streetPostdirection], 
                        [secondaryNumber], 
                        [city], 
                        [state], 
                        [zipcode], 
                        [plus4Code] 
                    ORDER BY [date] ASC
                ) AS rn
            FROM OPENROWSET(
                -- OPENROWSET to load data from Azure Blob Storage
                BULK 'mover-data/{blob_type}-geocoded/*_*_*_*/*',
                DATA_SOURCE = 'sa_esquiremovers',
                FORMAT = 'PARQUET'
            ) AS [data]
            -- Filter for recent data based on a computed date
            WHERE DATEFROMPARTS(data.filepath(2), data.filepath(3), data.filepath(4)) >= DATEADD(week, -24, GETDATE())
        ) AS [data]
        -- Filter to get only the first row per partition
        WHERE [rn] = 1;
    """
