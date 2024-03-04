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
                        PARTITION BY day, advertiser_id, insertion_order_id, line_item_id, creative_id 
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
            WHERE 
                advertiser_id != 0
                AND
                insertion_order_id != 0
                AND
                insertion_order_name NOT LIKE 'Resold%'
                AND
                line_item_id != 0
                AND
                line_item_name != '--'
                AND
                creative_id != 0
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
    """,
    "creatives": """
        SELECT
            [id]
            ,[type]
            ,CONVERT(DATETIME, [created_on]) AS [created_on]
            ,CONVERT(DATETIME, [last_modified]) AS [last_modified]
            ,[name]
            ,[width]
            ,[height]
            ,[advertiser_id]
            ,[brand_id]
            ,[member_id]
            ,[publisher_id]
            ,[profile_id]
            ,[size_in_bytes]
            ,[click_action]
            ,[click_target]
            ,[currency]
            ,[content_source]
            ,[state]
            ,[click_track_result]
            ,[service]
            ,[click_url]
            ,[landing_page_url]
            ,[media_url]
            ,[media_url_secure]
            ,CONVERT(INT, CONVERT(FLOAT, [sla])) AS [sla]
            ,[ssl_status]
            ,[audit_feedback]
            ,[audit_status]
            ,[allow_audit]
            ,[allow_ssl_audit]
            ,[is_control]
            ,[is_expired]
            ,[is_hosted]
            ,[is_product]
            ,[is_prohibited]
            ,[is_self_audited]
            ,[no_adservers]
            ,[track_clicks]
            ,[use_dynamic_click_url]
            ,[brand] -- JSON
            ,[ios_ssl_audit] -- JSON
            ,[language] -- JSON
            ,[mobile] -- JSON
            ,[political] -- JSON
            ,[status] -- JSON
            ,[template] -- JSON
            ,[video_attribute] -- JSON
            ,[line_items] -- JSON List
            ,[media_assets] -- JSON List
            ,[pixels] -- JSON List
            ,[vendors] -- JSON List
            ,[content] -- JS
            ,[content_secure] -- JS
            ,[original_content] -- JS
            ,[original_content_secure] -- JS
        FROM (
            SELECT
                *
                ,ROW_NUMBER()
                    OVER(
                        PARTITION BY [id]
                        ORDER BY CONVERT(DATETIME, [last_modified]) DESC
                    ) AS rank
            FROM OPENROWSET(
                BULK 'general/xandr/deltas/creatives/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) WITH (
                -- [format] VARCHAR(MAX),
                -- [no_iframes] VARCHAR(MAX),
                -- [creative_upload_status] VARCHAR(MAX),
                -- [backup_upload_status] VARCHAR(MAX),
                -- [thirdparty_creative_id] VARCHAR(MAX),
                -- [thirdparty_campaign_id] VARCHAR(MAX),
                -- [thirdparty_page] VARCHAR(MAX),
                -- [facebook_audit_status] VARCHAR(MAX),
                -- [facebook_audit_feedback] VARCHAR(MAX),
                -- [pop_values] VARCHAR(MAX),
                [track_clicks] BIT,
                [click_action] VARCHAR(16),
                [click_target] VARCHAR(2048),
                -- [text_title] VARCHAR(MAX),
                -- [text_description] VARCHAR(MAX),
                -- [text_display_url] VARCHAR(MAX),
                [allow_ssl_audit] BIT,
                [no_adservers] BIT,
                [original_content] VARCHAR(MAX),
                [original_content_secure] VARCHAR(MAX),
                -- [media_subtypes] VARCHAR(MAX),
                [brand_id] BIGINT,
                [type] VARCHAR(16),
                [is_hosted] BIT,
                [currency] VARCHAR(8),
                -- [sla_eta] VARCHAR(MAX),
                -- [file_name] VARCHAR(MAX),
                [id] BIGINT,
                [name] VARCHAR(512),
                -- [code] VARCHAR(MAX),
                -- [code2] VARCHAR(MAX),
                -- [custom_macros] VARCHAR(MAX),
                [created_on] VARCHAR(24),
                [last_modified] VARCHAR(24),
                [content_source] VARCHAR(16),
                [use_dynamic_click_url] BIT,
                [status] VARCHAR(MAX),
                [is_expired] BIT,
                [state] VARCHAR(16),
                [click_track_result] VARCHAR(24),
                [service] VARCHAR(24),
                [is_product] BIT,
                [advertiser_id] BIGINT,
                [publisher_id] BIGINT,
                [member_id] BIGINT,
                [profile_id] BIGINT,
                [line_items] VARCHAR(MAX),
                -- [thirdparty_pixels] VARCHAR(MAX),
                [template] VARCHAR(MAX),
                [media_assets] VARCHAR(MAX),
                [is_control] BIT,
                -- [custom_request_template] VARCHAR(MAX),
                -- [competitive_brands] VARCHAR(MAX),
                -- [competitive_categories] VARCHAR(MAX),
                -- [macros] VARCHAR(MAX),
                [brand] VARCHAR(MAX),
                [audit_status] VARCHAR(16),
                [audit_feedback] VARCHAR(256),
                [is_prohibited] BIT,
                [allow_audit] BIT,
                [is_self_audited] BIT,
                [ssl_status] VARCHAR(16),
                [language] VARCHAR(MAX),
                [ios_ssl_audit] VARCHAR(MAX),
                -- [adx_audit] VARCHAR(MAX),
                [sla] VARCHAR(16),
                [landing_page_url] VARCHAR(2048),
                [mobile] VARCHAR(MAX),
                [vendors] VARCHAR(MAX),
                [click_url] VARCHAR(MAX),
                [media_url] VARCHAR(MAX),
                [media_url_secure] VARCHAR(MAX),
                [content] VARCHAR(MAX),
                [content_secure] VARCHAR(MAX),
                [width] INT,
                [height] INT,
                -- [flash_backup_url] VARCHAR(2048),
                -- [flash_backup_url_secure] VARCHAR(2048),
                -- [flash_click_variable] VARCHAR(MAX),
                -- [segments] VARCHAR(MAX),
                [pixels] VARCHAR(MAX),
                [size_in_bytes] INT,
                -- [click_trackers] VARCHAR(MAX),
                -- [impression_trackers] VARCHAR(MAX),
                -- [native_attribute] VARCHAR(MAX),
                -- [thirdparty_viewability_providers] VARCHAR(MAX),
                [video_attribute] VARCHAR(MAX),
                [political] VARCHAR(MAX)
                -- [adchoices] VARCHAR(MAX),
                -- [native] VARCHAR(MAX)
            ) AS [data]
        ) AS [data]
        WHERE rank = 1
    """
}