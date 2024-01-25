def cetas_query_unique_deviceids(paths:str, handle:str) -> str:
    """
    Given one or more device observations files, build a query to extract the unique device ids.
    """
    return f"""
        SELECT DISTINCT [deviceid]
        FROM OPENROWSET(
            BULK '{paths}',
            DATA_SOURCE = '{handle}',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0',
            HEADER_ROW = TRUE
        ) WITH (
            [deviceid] VARCHAR(64),
            [timestamp] BIGINT,
            [lat] DECIMAL(11,6),
            [lng] DECIMAL(11,6)
        ) AS [data]
    """