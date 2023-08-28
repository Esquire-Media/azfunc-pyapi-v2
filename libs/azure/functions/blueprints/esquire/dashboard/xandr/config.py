from libs.data import register_binding, from_bind
import os

if not from_bind("xandr_dashboard"):
    register_binding(
        "xandr_dashboard",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_XANDR"],
        schemas=["dashboard"],
    )

CETAS = {
    "network_analytics": """
        SELECT
            CONVERT(DATE, [day]) AS [day],
            CONVERT(BIGINT, [advertiser_id]) AS [advertiser_id],
            [advertiser_name],
            CONVERT(BIGINT, [insertion_order_id]) AS [insertion_order_id],
            [insertion_order_name],
            CONVERT(BIGINT, [line_item_id]) AS [line_item_id],
            [line_item_name],
            CONVERT(BIGINT, [creative_id]) AS [creative_id],
            [creative_name],
            CONVERT(BIGINT, [clicks]) AS [clicks],
            CONVERT(BIGINT, [imps]) AS [imps],
            CONVERT(DECIMAL(11,6), [cost]) AS [cost],
            CONVERT(DECIMAL(11,6), [revenue]) AS [revenue]
        FROM (
            SELECT
                *,
                ROW_NUMBER()
                    OVER(
                        PARTITION BY day, line_item_id 
                        ORDER BY CONVERT(DATETIME2,data.filepath(1)) DESC
                    ) AS rank
            FROM OPENROWSET(
                BULK 'general/xandr/deltas/network_analytics/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) WITH (
                [day] VARCHAR(10),
                [advertiser_id] VARCHAR(16),
                [advertiser_name] VARCHAR(128),
                [insertion_order_id] VARCHAR(16),
                [insertion_order_name] VARCHAR(128),
                [line_item_id] VARCHAR(16),
                [line_item_name] VARCHAR(128),
                [creative_id] VARCHAR(16),
                [creative_name] VARCHAR(128),
                [clicks] VARCHAR(8),
                [imps] VARCHAR(8),
                [cost] VARCHAR(12),
                [revenue] VARCHAR(12)
            ) AS [data]
            WHERE line_item_id != 0
        ) AS [data]
        WHERE rank = 1
    """,
    "network_site_domain_performance": """
        SELECT
            CONVERT(DATE, [day]) AS [day],
            CONVERT(BIGINT, [line_item_id]) AS [line_item_id],
            [site_domain],
            [mobile_application_id],
            [mobile_application_name],
            [supply_type],
            CONVERT(BIGINT, [imps]) AS [imps],
            CONVERT(BIGINT, [clicks]) AS [clicks]
        FROM (
            SELECT
                *,
                ROW_NUMBER()
                    OVER(
                        PARTITION BY day, line_item_id, site_domain, mobile_application_id
                        ORDER BY CONVERT(DATETIME2,data.filepath(1)) DESC
                    ) AS rank
            FROM OPENROWSET(
                BULK 'general/xandr/deltas/network_site_domain_performance/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) WITH (
                [day] VARCHAR(10),
                [line_item_id] VARCHAR(16),
                [site_domain] VARCHAR(512),
                [mobile_application_id] VARCHAR(256),
                [mobile_application_name] VARCHAR(128),
                [supply_type] VARCHAR(16),
                [imps] VARCHAR(16),
                [clicks] VARCHAR(16)
            ) AS [data]
            WHERE line_item_id != 0
        ) AS [data]
        WHERE rank = 1
    """,
    "buyer_approximate_unique_users_hourly": """
        SELECT
            CONVERT(DATE, [day]) AS [day],
            CONVERT(BIGINT, [line_item_id]) AS [line_item_id],
            CONVERT(BIGINT, [creative_id]) AS [creative_id],
            CONVERT(BIGINT, [identified_imps]) AS [identified_imps],
            CONVERT(BIGINT, [unidentified_imps]) AS [unidentified_imps],
            CONVERT(BIGINT, [approx_users_count]) AS [approx_users_count],
            CONVERT(NUMERIC, [estimated_people_reach]) AS [estimated_people_reach]
        FROM (
            SELECT
                *,
                ROW_NUMBER()
                    OVER(
                        PARTITION BY day, line_item_id, creative_id
                        ORDER BY CONVERT(DATETIME2,data.filepath(1)) DESC
                    ) AS rank
            FROM OPENROWSET(
                BULK 'general/xandr/deltas/buyer_approximate_unique_users_hourly/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) WITH (
                [day] VARCHAR(10),
                [line_item_id] VARCHAR(16),
                [creative_id] VARCHAR(16),
                [identified_imps] VARCHAR(16),
                [unidentified_imps] VARCHAR(16),
                [approx_users_count] VARCHAR(16),
                [estimated_people_reach] VARCHAR(16)
            ) AS [data]
            WHERE line_item_id != 0
        ) AS [data]
        WHERE rank = 1
    """
}