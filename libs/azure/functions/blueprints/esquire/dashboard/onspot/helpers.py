# File: libs/azure/functions/blueprints/esquire/dashboard/onspot/helpers.py

def cetas_query_unique_deviceids(instance_id):
    return f"""
        SELECT DISTINCT [deviceid]
        FROM OPENROWSET(
            BULK 'dashboard/raw/{instance_id}/observations/*',
            DATA_SOURCE = 'sa_esquireonspot',
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

def cetas_query_sisense(instance_id):
    return f"""
        SELECT DISTINCT
            [O].[location_id],
            [O].[deviceid] AS [did],
            [O].[timestamp],
            [O].[latitude],
            [O].[longitude],
            [Z].[zipcode] AS [zip]
        FROM (
            SELECT
                [location_id],
                [deviceid],
                DATEADD(
                    HOUR,
                    DATEPART(HOUR,[start]),
                    CAST(CAST([start] AS DATE) AS DATETIME2)
                ) AS [timestamp],
                AVG([latitude]) AS [latitude],
                AVG([longitude]) AS [longitude]
            FROM (
                SELECT 
                    data.filepath(1) AS [location_id],
                    [deviceid],
                    [lat] AS [latitude],
                    [lng] AS [longitude],
                    CAST(DATEADD(second, [timestamp]/1000, '1970-01-01') AS DATETIME) AS [start]
                FROM OPENROWSET(
                    BULK 'dashboard/raw/{instance_id}/observations/*',
                    DATA_SOURCE = 'sa_esquireonspot',
                    FORMAT = 'CSV',
                    PARSER_VERSION = '2.0',
                    HEADER_ROW = TRUE
                ) WITH (
                    [deviceid] VARCHAR(64),
                    [timestamp] BIGINT,
                    [lat] DECIMAL(11,6),
                    [lng] DECIMAL(11,6)
                ) AS [data]
            ) [observations]
            GROUP BY 
                [location_id],
                [deviceid],
                CAST([start] AS DATE),
                DATEPART(HOUR, [start])
        ) AS [O]
        LEFT JOIN (
            SELECT *
            FROM OPENROWSET(
                BULK 'dashboard/raw/{instance_id}/zips/*',
                DATA_SOURCE = 'sa_esquireonspot',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                HEADER_ROW = TRUE
            ) WITH (
                [deviceid] VARCHAR(64),
                [zipcode] NCHAR(5)
            ) AS [data]
        ) [Z]
            ON [O].[deviceid] = [Z].[deviceid]
    """