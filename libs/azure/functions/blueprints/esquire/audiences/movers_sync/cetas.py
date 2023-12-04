def create_cetas_query(blob_type:list):
    """
    Generate a query to build a new CETAS statement.
    """

    # This CETAS query also dedupes before building
    # NOTE: Partition excludes secondary_designator, as we want "Unit 1" and "Apt #1" to count as the same address
    query = f"""
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
        SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY [primaryNumber], [streetPredirection], [streetName], [streetSuffix], [streetPostdirection], [secondaryNumber], [city], [state], [zipcode], [plus4Code] ORDER BY [date] ASC) AS rn
        FROM (
            SELECT
                CAST([date] AS DATE) AS [date],
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
            FROM OPENROWSET(
                BULK 'mover-data/{blob_type}-geocoded/*_*_*_*/*',
                DATA_SOURCE = 'sa_esquiremovers',
                FORMAT = 'PARQUET'
            ) AS [data]
            WHERE DATEFROMPARTS(data.filepath(2), data.filepath(3), data.filepath(4)) >= DATEADD(week, -24, GETDATE())
        ) AS [data]
    ) AS [data]
    WHERE [rn] = 1;
    """

    return query
