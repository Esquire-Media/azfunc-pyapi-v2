from libs.data import register_binding, from_bind
import os

if not from_bind("facebook_dashboard"):
    register_binding(
        "facebook_dashboard",
        "Structured",
        "sql",
        url=os.environ["DATABIND_SQL_FACEBOOK"],
        schemas=["dashboard"],
    )

PARAMETERS = {
    "User.Get.Adaccounts": {
        "limit": 100,
        "fields": [
            "account_id",
            "account_status",
            "age",
            # "agency_client_declaration",
            # "amount_spent",
            "balance",
            # "business", # Business
            "business_city",
            "business_country_code",
            "business_name",
            "business_state",
            "business_street",
            "business_street2",
            "business_zip",
            # "can_create_brand_lift_study",
            # "capabilities",
            "created_time",
            "currency",
            # "default_dsa_beneficiary",
            # "default_dsa_payor",
            "disable_reason",
            "end_advertiser",
            "end_advertiser_name",
            # "existing_customers",
            "fb_entity",
            "funding_source",
            "has_migrated_permissions",
            # "has_page_authorized_adaccount", # Page
            "id",
            # "io_number",
            "is_attribution_spec_system_default",
            "is_direct_deals_enabled",
            "is_in_3ds_authorization_enabled_market",
            "is_notifications_enabled",
            "is_personal",
            "is_prepay_account",
            "is_tax_id_required",
            # "line_numbers",
            # "media_agency",
            "min_campaign_group_spend_cap",
            "min_daily_budget",
            "name",
            "offsite_pixels_tos_accepted",
            "owner",
            # "owner_business", # N/A
            # "partner",
            # "show_checkout_experience", # Page
            "spend_cap",
            "tax_id",
            "tax_id_status",
            "tax_id_type",
            "timezone_id",
            "timezone_name",
            "timezone_offset_hours_utc",
            # "tos_accepted",
            # "user_tasks",
            # "user_tos_accepted",
            # "viewable_business" # N/A
        ],
    },
    "AdAccount.Post.Insights": {
        "fields": [
            # "account_currency",
            # "account_id",
            # "account_name",
            "ad_id",
            # "ad_name",
            # "adset_end",
            # "adset_id",
            # "adset_name",
            # "adset_start",
            # "age_targeting",
            # "attribution_setting", # Unused
            # "auction_bid", # Unused
            # "auction_competitiveness",
            # "auction_max_competitor_bid",
            "buying_type",
            # "campaign_id",
            # "campaign_name",
            # "canvas_avg_view_percent",
            # "canvas_avg_view_time",
            "clicks",
            # "conversion_rate_ranking",
            # "cost_per_dda_countby_convs",
            # "cost_per_estimated_ad_recallers",
            # "cost_per_inline_link_click",
            # "cost_per_inline_post_engagement",
            # "cost_per_unique_click",
            # "cost_per_unique_inline_link_click",
            "cpc",
            "cpm",
            "cpp",
            "created_time",
            "creative_media_type",
            "ctr",
            "date_start",
            "date_stop",
            # "dda_countby_convs",
            # "dda_results",
            # "engagement_rate_ranking",
            # "estimated_ad_recall_rate",
            # "estimated_ad_recall_rate_lower_bound",
            # "estimated_ad_recall_rate_upper_bound",
            # "estimated_ad_recallers",
            # "estimated_ad_recallers_lower_bound",
            # "estimated_ad_recallers_upper_bound",
            "frequency",
            # "full_view_impressions", # N/A
            # "full_view_reach", # N/A
            # "gender_targeting",
            "impressions",
            # "inline_link_click_ctr",
            # "inline_link_clicks",
            # "inline_post_engagement",
            # "instagram_upcoming_event_reminders_set",
            # "instant_experience_clicks_to_open",
            # "instant_experience_clicks_to_start",
            # "labels", # Unused
            # "location",
            "objective",
            "optimization_goal",
            # "place_page_name",
            # "qualifying_question_qualify_answer_rate",
            # "quality_ranking",
            # "quality_score_ectr",
            # "quality_score_ecvr",
            # "quality_score_organic",
            "reach",
            # "social_spend",
            "spend",
            # "total_postbacks",
            # "total_postbacks_detailed_v4",
            "unique_clicks",
            # "unique_ctr",
            # "unique_inline_link_click_ctr",
            # "unique_inline_link_clicks",
            # "unique_link_clicks_ctr",
            "updated_time",  # Filter
            # "video_play_curve_actions",
            # "video_play_retention_0_to_15s_actions",
            # "video_play_retention_20_to_60s_actions",
            # "video_play_retention_graph_actions",
            # "wish_bid",
        ],
        "level": "ad",
        "action_breakdowns": [
            "action_type",
            "action_target_id",
            "action_destination",
        ],
        "breakdowns": [
            # "ad_format_asset",
            "age",
            # "app_id",
            # "body_asset",
            # "call_to_action_asset",
            # "coarse_conversion_value",
            # "country",
            # "description_asset",
            # "device_platform",
            # "dma",
            # "fidelity_type",
            # "frequency_value",
            "gender",
            # "hourly_stats_aggregated_by_advertiser_time_zone",
            # "hourly_stats_aggregated_by_audience_time_zone",
            # "hsid",
            # "image_asset",
            # "impression_device",
            # "is_conversion_id_modeled",
            # "link_url_asset",
            # "mmm",
            # "place_page_id",
            # "platform_position",
            # "postback_sequence_index",
            # "product_id",
            # "publisher_platform",
            # "redownload",
            # "region",
            # "skan_campaign_id",
            # "skan_conversion_id",
            # "title_asset",
            # "video_asset"
        ],
        "time_increment": 1,
        # "action_attribution_windows": [
        #     # "dda",
        #     # "skan_click",
        #     # "skan_view",
        #     "1d_click",
        #     "1d_view",
        #     "28d_click",
        #     "28d_view",
        #     "7d_click",
        #     "7d_view",
        #     # "default",
        # ],
        "date_preset": os.environ.get("META_REPORTING_DATE_PRESET", "last_7d"),
        # "time_range": {"since": "2023-11-01", "until": "2023-11-30"},
    },
    "AdAccount.Get.Ads": {
        "limit": 200,
        "fields": [
            "account_id",
            "adset_id",
            "audience_id",
            "bid_amount",
            "bid_info",
            "bid_type",
            "campaign_id",
            "configured_status",
            "conversion_domain",
            "created_time",
            "date_format",
            "demolink_hash",
            "display_sequence",
            "draft_adgroup_id",
            "effective_status",
            "engagement_audience",
            "execution_options",
            "filename",
            "id",
            "include_demolink_hashes",
            "last_updated_by_app_id",
            # "meta_reward_adgroup_status",
            "name",
            "priority",
            # "source_ad",
            "source_ad_id",
            "status",
            "updated_time",  # Filter
        ],
        "date_preset": os.environ.get("META_REPORTING_DATE_PRESET", "last_7d"),
        # "time_range": {"since": "2023-01-01", "until": "2023-12-18"},
    },
    "AdAccount.Get.Campaigns": {
        "limit": 200,
        "fields": [
            "account_id",
            "adbatch",
            "bid_strategy",
            "boosted_object_id",
            "budget_rebalance_flag",
            "budget_remaining",
            "buying_type",
            "can_create_brand_lift_study",
            "can_use_spend_cap",
            "configured_status",
            "created_time",
            "daily_budget",
            "effective_status",
            "execution_options",
            "has_secondary_skadnetwork_reporting",
            "id",
            "is_skadnetwork_attribution",
            "iterative_split_test_configs",
            "last_budget_toggling_time",
            "lifetime_budget",
            "name",
            "objective",
            "pacing_type",
            "primary_attribution",
            "smart_promotion_type",
            # "source_campaign",
            "source_campaign_id",
            "special_ad_categories",
            "special_ad_category",
            "special_ad_category_country",
            "spend_cap",
            "start_time",
            "status",
            "stop_time",
            "topline_id",
            "updated_time",  # Filter
        ],
        "date_preset": os.environ.get("META_REPORTING_DATE_PRESET", "last_7d"),
        # "time_range": {"since": "2023-01-01", "until": "2023-12-18"},
    },
    "AdAccount.Get.Adsets": {
        "limit": 200,
        "fields": [
            "account_id",
            # "asset_feed_id", # Empty?
            "bid_amount",
            "bid_info",
            "bid_strategy",
            "billing_event",
            "budget_remaining",
            "campaign_attribution",
            "campaign_id",
            "campaign_spec",
            "configured_status",
            "created_time",
            "creative_sequence",
            "daily_budget",
            # "daily_imps", # N/A
            "daily_min_spend_target",
            "daily_spend_cap",
            "date_format",
            "destination_type",
            "dsa_beneficiary",
            "dsa_payor",
            "effective_status",
            "end_time",
            "execution_options",
            # "existing_customer_budget_percentage", # Permission
            # "full_funnel_exploration_mode", # Account and App must be whitelisted
            "id",
            "instagram_actor_id",
            "is_dynamic_creative",
            "lifetime_budget",
            "lifetime_imps",
            "lifetime_min_spend_target",
            "lifetime_spend_cap",
            # "line_number", # N/A
            "multi_optimization_goal_weight",
            "name",
            "optimization_goal",
            "optimization_sub_event",
            "pacing_type",
            "rb_prediction_id",
            "recurring_budget_semantics",
            "review_feedback",
            "rf_prediction_id",
            # "source_adset",
            "source_adset_id",
            "start_time",
            "status",
            "targeting_optimization_types",
            "time_based_ad_rotation_id_blocks",
            "time_based_ad_rotation_intervals",
            "time_start",
            "time_stop",
            # "topline_id", # N/A
            "tune_for_category",
            "updated_time",  # Filter
            "use_new_app_click",
        ],
        "date_preset": os.environ.get("META_REPORTING_DATE_PRESET", "last_7d"),
        # "time_range": {"since": "2023-01-01", "until": "2023-12-18"},
    },
    "AdAccount.Get.Adcreatives": {
        "limit": 100,
        "fields": [
            "account_id",
            "actor_id",
            "applink_treatment",
            "authorization_category",
            "auto_update",
            "body",
            "branded_content_sponsor_page_id",
            "bundle_folder_id",
            # "call_to_action", # N/A
            "call_to_action_type",
            "categorization_criteria",
            "category_media_source",
            "collaborative_ads_lsb_image_bank_id",
            "destination_set_id",
            "dynamic_ad_voice",
            "effective_authorization_category",
            "effective_instagram_media_id",
            "effective_instagram_story_id",
            "effective_object_story_id",
            "enable_direct_install",
            # "enable_launch_instant_app", # Permission issue
            "id",
            "image_file",
            "image_hash",
            "image_url",
            # "instagram_actor_id",
            # "instagram_permalink_url",
            # "instagram_story_id",
            # "instagram_user_id",
            "is_dco_internal",
            "link_deep_link_url",
            "link_destination_display_url",
            "link_og_id",
            "link_url",
            "messenger_sponsored_message",
            "name",
            "object_id",
            "object_store_url",
            "object_story_id",
            "object_type",
            "object_url",
            "place_page_set_id",
            "playable_asset_id",
            "product_set_id",
            "source_instagram_media_id",
            "status",
            "template_url",
            "thumbnail_id",
            "thumbnail_url",
            "title",
            "url_tags",
            "use_page_actor_override",
            "video_id",
        ],
    },
}


CETAS = {
    "User.Get.Adaccounts": """
        WITH 
        latest_date AS (
            SELECT MAX(data.filepath(1)) AS max_date
            FROM OPENROWSET(
                BULK 'general/meta/delta/adaccounts/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) AS data
        ),
        new_data AS (    
            SELECT 
                CAST(account_id AS BIGINT) AS account_id,
                CAST(account_status AS INT) AS account_status,
                CAST(age AS FLOAT) AS age,
                -- CAST(amount_spent AS BIGINT) AS amount_spent,
                -- CAST(balance AS BIGINT) AS balance,
                business_city,
                business_country_code,
                business_name,
                business_state,
                business_street,
                business_street2,
                business_zip,
                -- CAST(can_create_brand_lift_study AS BIT) AS can_create_brand_lift_study,
                -- capabilities,
                CAST(
                    CONCAT(
                        LEFT(created_time, 22),
                        ':',
                        RIGHT(created_time, 2)
                    ) AS DATETIMEOFFSET
                ) AS created_time,
                currency,
                -- default_dsa_beneficiary,
                -- default_dsa_payor,
                disable_reason,
                CAST(end_advertiser AS BIGINT) AS end_advertiser,
                end_advertiser_name,
                -- existing_customers,
                CAST(fb_entity AS BIGINT) AS fb_entity,
                CAST(funding_source AS BIGINT) AS funding_source,
                CAST(has_migrated_permissions AS BIT) AS has_migrated_permissions,
                id,
                -- io_number,
                CAST(is_attribution_spec_system_default AS BIT) AS is_attribution_spec_system_default,
                CAST(is_direct_deals_enabled AS BIT) AS is_direct_deals_enabled,
                CAST(is_in_3ds_authorization_enabled_market AS BIT) AS is_in_3ds_authorization_enabled_market,
                CAST(is_notifications_enabled AS BIT) AS is_notifications_enabled,
                CAST(is_personal AS BIT) AS is_personal,
                CAST(is_prepay_account AS BIT) AS is_prepay_account,
                CAST(is_tax_id_required AS BIT) AS is_tax_id_required,
                -- line_numbers,
                -- media_agency,
                CAST(min_campaign_group_spend_cap AS BIGINT) AS min_campaign_group_spend_cap,
                CAST(min_daily_budget AS BIGINT) AS min_daily_budget,
                name,
                CAST(offsite_pixels_tos_accepted AS BIT) AS offsite_pixels_tos_accepted,
                CAST(owner AS BIGINT) AS owner,
                -- partner,
                CAST(spend_cap AS BIGINT) AS spend_cap,
                tax_id,
                CAST(tax_id_status AS INT) AS tax_id_status,
                CAST(tax_id_type AS BIGINT) AS tax_id_type,
                CAST(timezone_id AS INT) AS timezone_id,
                timezone_name,
                CAST(timezone_offset_hours_utc AS NUMERIC) AS timezone_offset_hours_utc
                -- tos_accepted,
                -- user_tasks,
                -- user_tos_accepted
            FROM OPENROWSET(
                BULK 'general/meta/delta/adaccounts/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) WITH (
                account_id VARCHAR(32),
                account_status VARCHAR(16),
                age VARCHAR(16),
                -- amount_spent VARCHAR(16),
                -- balance VARCHAR(16),
                business_city VARCHAR(128),
                business_country_code VARCHAR(4),
                business_name VARCHAR(128),
                business_state VARCHAR(32),
                business_street VARCHAR(128),
                business_street2 VARCHAR(128),
                business_zip VARCHAR(5),
                -- can_create_brand_lift_study VARCHAR(1),
                -- capabilities VARCHAR(MAX),
                created_time VARCHAR(32),
                currency VARCHAR(8),
                -- default_dsa_beneficiary VARCHAR(32),
                -- default_dsa_payor VARCHAR(32),
                disable_reason VARCHAR(16),
                end_advertiser VARCHAR(32),
                end_advertiser_name VARCHAR(128),
                -- existing_customers VARCHAR(128),
                fb_entity VARCHAR(32),
                funding_source VARCHAR(32),
                has_migrated_permissions VARCHAR(1),
                id VARCHAR(32),
                -- io_number VARCHAR(32),
                is_attribution_spec_system_default VARCHAR(1),
                is_direct_deals_enabled VARCHAR(1),
                is_in_3ds_authorization_enabled_market VARCHAR(1),
                is_notifications_enabled VARCHAR(1),
                is_personal VARCHAR(1),
                is_prepay_account VARCHAR(1),
                is_tax_id_required VARCHAR(1),
                -- line_numbers VARCHAR(32),
                -- media_agency VARCHAR(32),
                min_campaign_group_spend_cap VARCHAR(16),
                min_daily_budget VARCHAR(16),
                name VARCHAR(128),
                offsite_pixels_tos_accepted VARCHAR(1),
                owner VARCHAR(32),
                -- partner VARCHAR(32),
                spend_cap VARCHAR(16),
                tax_id VARCHAR(16),
                tax_id_status VARCHAR(1),
                tax_id_type VARCHAR(4),
                timezone_id VARCHAR(4),
                timezone_name VARCHAR(32),
                timezone_offset_hours_utc VARCHAR(16)
                -- tos_accepted VARCHAR(1024),
                -- user_tasks VARCHAR(1024),
                -- user_tos_accepted VARCHAR(1024)
            ) AS [data]
            WHERE data.filepath(1) = (SELECT max_date from latest_date)
        )
        SELECT * FROM new_data
        UNION
        SELECT 
            account_id,
            account_status,
            age,
            -- amount_spent,
            -- balance,
            business_city,
            business_country_code,
            business_name,
            business_state,
            business_street,
            business_street2,
            business_zip,
            -- can_create_brand_lift_study,
            -- capabilities,
            created_time,
            currency,
            -- default_dsa_beneficiary,
            -- default_dsa_payor,
            disable_reason,
            end_advertiser,
            end_advertiser_name,
            -- existing_customers,
            fb_entity,
            funding_source,
            has_migrated_permissions,
            id,
            -- io_number,
            is_attribution_spec_system_default,
            is_direct_deals_enabled,
            is_in_3ds_authorization_enabled_market,
            is_notifications_enabled,
            is_personal,
            is_prepay_account,
            is_tax_id_required,
            -- line_numbers,
            -- media_agency,
            min_campaign_group_spend_cap,
            min_daily_budget,
            name,
            offsite_pixels_tos_accepted,
            owner,
            -- partner,
            spend_cap,
            tax_id,
            tax_id_status,
            tax_id_type,
            timezone_id,
            timezone_name,
            timezone_offset_hours_utc
            -- tos_accepted,
            -- user_tasks,
            -- user_tos_accepted
        FROM dashboard.adaccounts
        WHERE NOT EXISTS (
            SELECT 1
            FROM new_data
            WHERE new_data.id = dashboard.adaccounts.id
        )

    """,
    "AdAccount.Post.Insights": """
        WITH 
        latest_date AS (
            SELECT MAX(data.filepath(1)) AS max_date
            FROM OPENROWSET(
                BULK 'general/meta/delta/adsinsights/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) AS data
        ),
        new_reports AS (
            SELECT
                CAST([Ad ID] AS BIGINT) AS ad_id,
                [Buying Type] AS buying_type,
                CAST([Clicks (all)] AS BIGINT) AS clicks,
                CAST([CPC (All) (USD)] AS NUMERIC) AS cpc,
                CAST([CPM (cost per 1,000 impressions) (USD)] AS NUMERIC) AS cpm,
                CAST([Cost per 1,000 Accounts Center accounts reached (USD)] AS NUMERIC) AS cpp,
                CAST([Date created] AS DATE) AS created_time,
                [Media type] AS creative_media_type,
                CAST([CRT (all)] AS NUMERIC) AS ctr,
                CAST([Reporting starts] AS DATE) AS date_start,
                CAST([Reporting ends] AS DATE) AS date_end,
                CAST([Frequency] AS NUMERIC) AS frequency,
                CAST([Impressions] AS BIGINT) AS impressions,
                [Objective] AS objective,
                [Optimization goal] AS optimization_goal,
                CAST([Reach] AS BIGINT) AS reach,
                CAST([Amount spent (USD)] AS NUMERIC) AS spend,
                CAST([Unique clicks (all)] AS BIGINT) AS unique_clicks,
                CAST([Date last edited] AS DATE) AS updated_time,
                [Age] AS age_range,
                [Gender] AS gender
            FROM OPENROWSET(
                BULK 'general/meta/delta/adsinsights/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',  
                FORMAT = 'PARQUET' 
            ) WITH (
                [Reporting starts] VARCHAR(10),
                [Reporting ends] VARCHAR(10),
                [Age] VARCHAR(8),
                [Gender] VARCHAR(8),
                [Ad ID] VARCHAR(24),
                [Buying Type] VARCHAR(16),
                [Clicks (all)] VARCHAR(16),
                [CPC (All) (USD)] VARCHAR(16),
                [CPM (cost per 1,000 impressions) (USD)] VARCHAR(16),
                [Cost per 1,000 Accounts Center accounts reached (USD)] VARCHAR(16),
                [Date created] VARCHAR(10),
                [Media type] VARCHAR(16),
                [CRT (all)] VARCHAR(16),
                [Frequency] VARCHAR(16),
                [Impressions] VARCHAR(16),
                [Objective] VARCHAR(MAX),
                [Optimization goal] VARCHAR(32),
                [Reach] VARCHAR(16),
                [Amount spent (USD)] VARCHAR(16),
                [Unique clicks (all)] VARCHAR(16),
                [Date last edited] VARCHAR(10)
            ) AS data
            WHERE data.filepath(1) = (SELECT max_date from latest_date)
        ),
        new_reports_fallback AS (
            SELECT
                CAST(ad_id AS BIGINT) AS ad_id,
                buying_type,
                CAST(clicks AS BIGINT) AS clicks,
                CAST(cpc AS NUMERIC) AS cpc,
                CAST(cpm AS NUMERIC) AS cpm,
                CAST(cpp AS NUMERIC) AS cpp,
                CAST(created_time AS DATE) AS created_time,
                creative_media_type,
                CAST(ctr AS NUMERIC) AS ctr,
                CAST(date_start AS DATE) AS date_start,
                CAST(date_stop AS DATE) AS date_end,
                CAST(frequency AS NUMERIC) AS frequency,
                CAST(impressions AS BIGINT) AS impressions,
                objective AS objective,
                optimization_goal AS optimization_goal,
                CAST(reach AS BIGINT) AS reach,
                CAST(spend AS NUMERIC) AS spend,
                CAST(unique_clicks AS BIGINT) AS unique_clicks,
                CAST(updated_time AS DATE) AS updated_time,
                age AS age_range,
                gender
            FROM OPENROWSET(
                BULK 'general/meta/delta/adsinsights/*/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',  
                FORMAT = 'PARQUET' 
            ) WITH (
                ad_id VARCHAR(24),
                buying_type VARCHAR(16),
                clicks VARCHAR(16),
                cpm VARCHAR(16),
                cpp VARCHAR(16),
                created_time VARCHAR(10),
                creative_media_type VARCHAR(16),
                ctr VARCHAR(16),
                date_start VARCHAR(10),
                date_stop VARCHAR(10),
                frequency VARCHAR(16),
                impressions VARCHAR(16),
                objective VARCHAR(MAX),
                optimization_goal VARCHAR(32),
                reach VARCHAR(16),
                spend VARCHAR(16),
                unique_clicks VARCHAR(16),
                updated_time VARCHAR(10),
                age VARCHAR(8),
                gender VARCHAR(8),
                cpc VARCHAR(16)
            ) AS data
            WHERE data.filepath(1) = (SELECT max_date from latest_date)
        ),
        new_data AS (
            SELECT * FROM new_reports
            UNION
            SELECT * FROM new_reports_fallback
        )
        SELECT * FROM new_data
        UNION
        SELECT 
            ad_id,
            buying_type,
            clicks,
            cpc,
            cpm,
            cpp,
            created_time,
            creative_media_type,
            ctr,
            date_start,
            date_end,
            frequency,
            impressions,
            objective,
            optimization_goal,
            reach,
            spend,
            unique_clicks,
            updated_time,
            age_range,
            gender
        FROM dashboard.adsinsights
        WHERE NOT EXISTS (
            SELECT 1
            FROM new_data
            WHERE new_data.ad_id = dashboard.adsinsights.ad_id
            AND new_data.date_start = dashboard.adsinsights.date_start
        )
    """,
    "AdAccount.Get.Ads": """
        WITH 
        latest_date AS (
            SELECT MAX(data.filepath(1)) AS max_date
            FROM OPENROWSET(
                BULK 'general/meta/delta/ads/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) AS data
        ),
        new_data AS (    
            SELECT
                CAST(account_id AS BIGINT) AS account_id,
                CAST(adset_id AS BIGINT) AS adset_id,
                CAST(audience_id AS BIGINT) AS audience_id,
                CAST(bid_amount AS NUMERIC) AS bid_amount,
                bid_info,
                bid_type,
                CAST(campaign_id AS BIGINT) AS campaign_id,
                configured_status,
                conversion_domain,
                TRY_CAST(
                    CONCAT(
                        LEFT(
                            created_time, 
                            22
                        ),
                        ':',
                        RIGHT(
                            created_time,
                            2
                        )
                    ) AS DATETIMEOFFSET
                ) AS created_time,
                date_format,
                demolink_hash,
                display_sequence,
                draft_adgroup_id,
                effective_status,
                engagement_audience,
                execution_options,
                filename,
                CAST(id AS BIGINT) AS id,
                include_demolink_hashes,
                CAST(last_updated_by_app_id AS BIGINT) AS last_updated_by_app_id,
                meta_reward_adgroup_status,
                name,
                priority,
                CAST(source_ad_id AS BIGINT) AS source_ad_id,
                status,
                TRY_CAST(
                    CONCAT(
                        LEFT(
                            updated_time, 
                            22
                        ),
                        ':',
                        RIGHT(
                            updated_time,
                            2
                        )
                    ) AS DATETIMEOFFSET
                ) AS updated_time
            FROM OPENROWSET(
                BULK 'general/meta/delta/ads/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) WITH (
                account_id VARCHAR(32),
                adset_id VARCHAR(32),
                audience_id VARCHAR(32),
                bid_amount VARCHAR(16),
                bid_info VARCHAR(1024),
                bid_type VARCHAR(16),
                campaign_id VARCHAR(32),
                configured_status VARCHAR(MAX),
                conversion_domain VARCHAR(MAX),
                created_time VARCHAR(32),
                date_format VARCHAR(16),
                demolink_hash VARCHAR(32),
                display_sequence VARCHAR(32),
                draft_adgroup_id VARCHAR(32),
                effective_status VARCHAR(32),
                engagement_audience VARCHAR(32),
                execution_options VARCHAR(32),
                filename VARCHAR(128),
                id VARCHAR(32),
                include_demolink_hashes VARCHAR(1),
                last_updated_by_app_id VARCHAR(32),
                meta_reward_adgroup_status VARCHAR(16),
                name VARCHAR(256),
                priority VARCHAR(4),
                source_ad_id VARCHAR(32),
                status VARCHAR(16),
                updated_time VARCHAR(32)
            ) AS [data]
            WHERE data.filepath(1) = (SELECT max_date from latest_date)
        )
        SELECT * FROM new_data
        UNION
        SELECT 
            account_id,
            adset_id,
            audience_id,
            bid_amount,
            bid_info,
            bid_type,
            campaign_id,
            configured_status,
            conversion_domain,
            created_time,
            date_format,
            demolink_hash,
            display_sequence,
            draft_adgroup_id,
            effective_status,
            engagement_audience,
            execution_options,
            filename,
            id,
            include_demolink_hashes,
            last_updated_by_app_id,
            meta_reward_adgroup_status,
            name,
            priority,
            source_ad_id,
            status,
            updated_time
        FROM dashboard.ads
        WHERE NOT EXISTS (
            SELECT 1
            FROM new_data
            WHERE new_data.id = dashboard.ads.id
        )
    """,
    "AdAccount.Get.Campaigns": """
        WITH 
        latest_date AS (
            SELECT MAX(data.filepath(1)) AS max_date
            FROM OPENROWSET(
                BULK 'general/meta/delta/campaigns/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) AS data
        ),
        new_data AS (    
            SELECT
                CAST(account_id AS BIGINT) AS account_id,
                adbatch AS adbatch,
                bid_strategy AS bid_strategy,
                CAST(boosted_object_id AS BIGINT) AS boosted_object_id,
                CAST(budget_rebalance_flag AS BIT) AS budget_rebalance_flag,
                CAST(budget_remaining AS NUMERIC) AS budget_remaining,
                buying_type AS buying_type,
                CAST(can_create_brand_lift_study AS BIT) AS can_create_brand_lift_study,
                CAST(can_use_spend_cap AS BIT) AS can_use_spend_cap,
                configured_status AS configured_status,
                TRY_CAST(CONCAT(LEFT(created_time, 22), ':', RIGHT(created_time, 2)) AS DATETIMEOFFSET) AS created_time,
                CAST(daily_budget AS NUMERIC) AS daily_budget,
                effective_status AS effective_status,
                execution_options AS execution_options,
                CAST(has_secondary_skadnetwork_reporting AS BIT) AS has_secondary_skadnetwork_reporting,
                CAST(id AS BIGINT) AS id,
                CAST(is_skadnetwork_attribution AS BIT) AS is_skadnetwork_attribution,
                iterative_split_test_configs AS iterative_split_test_configs,
                last_budget_toggling_time AS last_budget_toggling_time,
                CAST(lifetime_budget AS NUMERIC) AS lifetime_budget,
                name AS name,
                objective AS objective,
                pacing_type AS pacing_type,
                primary_attribution AS primary_attribution,
                smart_promotion_type AS smart_promotion_type,
                CAST(source_campaign_id AS BIGINT) AS source_campaign_id,
                special_ad_categories AS special_ad_categories,
                special_ad_category AS special_ad_category,
                special_ad_category_country AS special_ad_category_country,
                CAST(spend_cap AS NUMERIC) AS spend_cap,
                TRY_CAST(CONCAT(LEFT(start_time, 22), ':', RIGHT(start_time, 2)) AS DATETIMEOFFSET) AS start_time,
                status AS status,
                TRY_CAST(CONCAT(LEFT(stop_time, 22), ':', RIGHT(stop_time, 2)) AS DATETIMEOFFSET) AS stop_time,
                CAST(topline_id AS BIGINT) AS topline_id,
                TRY_CAST(CONCAT(LEFT(updated_time, 22), ':', RIGHT(updated_time, 2)) AS DATETIMEOFFSET) AS updated_time
            FROM OPENROWSET(
                BULK 'general/meta/delta/campaigns/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) WITH (
                account_id VARCHAR(32),
                adbatch VARCHAR(16),
                bid_strategy VARCHAR(32),
                boosted_object_id VARCHAR(32),
                budget_rebalance_flag VARCHAR(1),
                budget_remaining VARCHAR(16),
                buying_type VARCHAR(16),
                can_create_brand_lift_study VARCHAR(1),
                can_use_spend_cap VARCHAR(1),
                configured_status VARCHAR(16),
                created_time VARCHAR(32),
                daily_budget VARCHAR(16),
                effective_status VARCHAR(16),
                execution_options VARCHAR(32),
                has_secondary_skadnetwork_reporting VARCHAR(1),
                id VARCHAR(32),
                is_skadnetwork_attribution VARCHAR(1),
                iterative_split_test_configs VARCHAR(16),
                last_budget_toggling_time VARCHAR(32),
                lifetime_budget VARCHAR(16),
                name VARCHAR(128),
                objective VARCHAR(32),
                pacing_type VARCHAR(16),
                primary_attribution VARCHAR(16),
                smart_promotion_type VARCHAR(16),
                source_campaign_id VARCHAR(32),
                special_ad_categories VARCHAR(1024),
                special_ad_category VARCHAR(128),
                special_ad_category_country VARCHAR(16),
                spend_cap VARCHAR(16),
                start_time VARCHAR(32),
                status VARCHAR(16),
                stop_time VARCHAR(32),
                topline_id VARCHAR(32),
                updated_time VARCHAR(32)
            ) AS [data]
            WHERE data.filepath(1) = (SELECT max_date from latest_date)
        )
        SELECT * FROM new_data
        UNION
        SELECT 
            account_id,
            adbatch,
            bid_strategy,
            boosted_object_id,
            budget_rebalance_flag,
            budget_remaining,
            buying_type,
            can_create_brand_lift_study,
            can_use_spend_cap,
            configured_status,
            created_time,
            daily_budget,
            effective_status,
            execution_options,
            has_secondary_skadnetwork_reporting,
            id,
            is_skadnetwork_attribution,
            iterative_split_test_configs,
            last_budget_toggling_time,
            lifetime_budget,
            name,
            objective,
            pacing_type,
            primary_attribution,
            smart_promotion_type,
            source_campaign_id,
            special_ad_categories,
            special_ad_category,
            special_ad_category_country,
            spend_cap,
            start_time,
            status,
            stop_time,
            topline_id,
            updated_time
        FROM dashboard.campaigns
        WHERE NOT EXISTS (
            SELECT 1
            FROM new_data
            WHERE new_data.id = dashboard.campaigns.id
        )
    """,
    "AdAccount.Get.Adsets": """
        WITH 
        latest_date AS (
            SELECT MAX(data.filepath(1)) AS max_date
            FROM OPENROWSET(
                BULK 'general/meta/delta/adsets/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) AS data
        ),
        new_data AS (    
            SELECT
                CAST(account_id AS BIGINT) AS account_id,
                CAST(bid_amount AS NUMERIC) AS bid_amount,
                bid_info,
                bid_strategy,
                billing_event,
                CAST(budget_remaining AS BIGINT) AS budget_remaining,
                campaign_attribution,
                CAST(campaign_id AS BIGINT) AS campaign_id,
                campaign_spec,
                configured_status,
                TRY_CAST(
                    CONCAT(
                        LEFT(
                            created_time, 
                            22
                        ),
                        ':',
                        RIGHT(
                            created_time,
                            2
                        )
                    ) AS DATETIMEOFFSET
                ) AS created_time,
                creative_sequence,
                CAST(daily_budget AS BIGINT) AS daily_budget,
                CAST(daily_min_spend_target AS BIGINT) AS daily_min_spend_target,
                CAST(daily_spend_cap AS BIGINT) AS daily_spend_cap,
                date_format,
                destination_type,
                dsa_beneficiary,
                dsa_payor,
                effective_status,
                end_time,
                execution_options,
                CAST(id AS BIGINT) AS id,
                instagram_actor_id,
                CAST(is_dynamic_creative AS BIT) AS is_dynamic_creative,
                CAST(lifetime_budget AS BIGINT) AS lifetime_budget,
                CAST(lifetime_imps AS BIGINT) AS lifetime_imps,
                CAST(lifetime_min_spend_target AS BIGINT) AS lifetime_min_spend_target,
                CAST(lifetime_spend_cap AS BIGINT) AS lifetime_spend_cap,
                multi_optimization_goal_weight,
                name,
                optimization_goal,
                optimization_sub_event,
                pacing_type,
                rb_prediction_id,
                recurring_budget_semantics,
                review_feedback,
                rf_prediction_id,
                source_adset_id,
                TRY_CAST(
                    CONCAT(
                        LEFT(
                            start_time, 
                            22
                        ),
                        ':',
                        RIGHT(
                            start_time,
                            2
                        )
                    ) AS DATETIMEOFFSET
                ) AS start_time,
                status,
                targeting_optimization_types,
                time_based_ad_rotation_id_blocks,
                time_based_ad_rotation_intervals,
                TRY_CAST(
                    CONCAT(
                        LEFT(
                            time_start, 
                            22
                        ),
                        ':',
                        RIGHT(
                            time_start,
                            2
                        )
                    ) AS DATETIMEOFFSET
                ) AS time_start,
                TRY_CAST(
                    CONCAT(
                        LEFT(
                            time_stop, 
                            22
                        ),
                        ':',
                        RIGHT(
                            time_stop,
                            2
                        )
                    ) AS DATETIMEOFFSET
                ) AS time_stop,
                tune_for_category,
                TRY_CAST(
                    CONCAT(
                        LEFT(
                            updated_time, 
                            22
                        ),
                        ':',
                        RIGHT(
                            updated_time,
                            2
                        )
                    ) AS DATETIMEOFFSET
                ) AS updated_time,
                CAST(use_new_app_click AS BIT) AS use_new_app_click
            FROM OPENROWSET(
                BULK 'general/meta/delta/adsets/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) WITH (
                account_id VARCHAR(32),
                bid_amount VARCHAR(16),
                bid_info VARCHAR(MAX),
                bid_strategy VARCHAR(32),
                billing_event VARCHAR(16),
                budget_remaining VARCHAR(16),
                campaign_attribution VARCHAR(16),
                campaign_id VARCHAR(32),
                campaign_spec VARCHAR(16),
                configured_status VARCHAR(16),
                created_time VARCHAR(32),
                creative_sequence VARCHAR(32),
                daily_budget VARCHAR(16),
                daily_min_spend_target VARCHAR(16),
                daily_spend_cap VARCHAR(16),
                date_format VARCHAR(16),
                destination_type VARCHAR(16),
                dsa_beneficiary VARCHAR(32),
                dsa_payor VARCHAR(32),
                effective_status VARCHAR(32),
                end_time VARCHAR(32),
                execution_options VARCHAR(1024),
                id VARCHAR(32),
                instagram_actor_id VARCHAR(32),
                is_dynamic_creative VARCHAR(1),
                lifetime_budget VARCHAR(16),
                lifetime_imps VARCHAR(16),
                lifetime_min_spend_target VARCHAR(16),
                lifetime_spend_cap VARCHAR(16),
                multi_optimization_goal_weight VARCHAR(16),
                name VARCHAR(256),
                optimization_goal VARCHAR(32),
                optimization_sub_event VARCHAR(32),
                pacing_type VARCHAR(16),
                rb_prediction_id VARCHAR(32),
                recurring_budget_semantics VARCHAR(32),
                review_feedback VARCHAR(1024),
                rf_prediction_id VARCHAR(32),
                source_adset_id VARCHAR(32),
                start_time VARCHAR(32),
                status VARCHAR(32),
                targeting_optimization_types VARCHAR(1024),
                time_based_ad_rotation_id_blocks VARCHAR(32),
                time_based_ad_rotation_intervals VARCHAR(32),
                time_start VARCHAR(32),
                time_stop VARCHAR(32),
                tune_for_category VARCHAR(32),
                updated_time VARCHAR(32),
                use_new_app_click VARCHAR(1)
            ) AS [data]
            WHERE data.filepath(1) = (SELECT max_date from latest_date)
        )
        SELECT * FROM new_data
        UNION
        SELECT 
            account_id,
            bid_amount,
            bid_info,
            bid_strategy,
            billing_event,
            budget_remaining,
            campaign_attribution,
            campaign_id,
            campaign_spec,
            configured_status,
            created_time,
            creative_sequence,
            daily_budget,
            daily_min_spend_target,
            daily_spend_cap,
            date_format,
            destination_type,
            dsa_beneficiary,
            dsa_payor,
            effective_status,
            end_time,
            execution_options,
            id,
            instagram_actor_id,
            is_dynamic_creative,
            lifetime_budget,
            lifetime_imps,
            lifetime_min_spend_target,
            lifetime_spend_cap,
            multi_optimization_goal_weight,
            name,
            optimization_goal,
            optimization_sub_event,
            pacing_type,
            rb_prediction_id,
            recurring_budget_semantics,
            review_feedback,
            rf_prediction_id,
            source_adset_id,
            start_time,
            status,
            targeting_optimization_types,
            time_based_ad_rotation_id_blocks,
            time_based_ad_rotation_intervals,
            time_start,
            time_stop,
            tune_for_category,
            updated_time,
            use_new_app_click
        FROM dashboard.adsets
        WHERE NOT EXISTS (
            SELECT 1
            FROM new_data
            WHERE new_data.id = dashboard.adsets.id
        )
    """,
    "AdAccount.Get.Adcreatives": """
        WITH 
        latest_date AS (
            SELECT MAX(data.filepath(1)) AS max_date
            FROM OPENROWSET(
                BULK 'general/meta/delta/adcreatives/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) AS data
        ),
        new_data AS (    
            SELECT 
                CAST(account_id AS BIGINT) AS account_id,
                CAST(actor_id AS BIGINT) AS actor_id,
                applink_treatment,
                authorization_category,
                auto_update,
                body,
                branded_content_sponsor_page_id,
                bundle_folder_id,
                call_to_action_type,
                categorization_criteria,
                category_media_source,
                collaborative_ads_lsb_image_bank_id,
                destination_set_id,
                dynamic_ad_voice,
                effective_authorization_category,
                CAST(effective_instagram_media_id AS BIGINT) AS effective_instagram_media_id,
                CAST(effective_instagram_story_id AS BIGINT) AS effective_instagram_story_id,
                effective_object_story_id,
                CAST(enable_direct_install AS BIT) AS enable_direct_install,
                CAST(id AS BIGINT) AS id,
                image_file,
                image_hash,
                image_url,
                CAST(is_dco_internal AS BIT) AS is_dco_internal,
                link_deep_link_url,
                link_destination_display_url,
                link_og_id,
                link_url,
                messenger_sponsored_message,
                name,
                object_id,
                object_store_url,
                object_story_id,
                object_type,
                object_url,
                place_page_set_id,
                playable_asset_id,
                product_set_id,
                source_instagram_media_id,
                status,
                template_url,
                CAST(thumbnail_id AS BIGINT) AS thumbnail_id,
                thumbnail_url,
                title,
                url_tags,
                CAST(use_page_actor_override AS BIT) AS use_page_actor_override,
                CAST(video_id AS BIGINT) AS video_id
            FROM OPENROWSET(
                BULK 'general/meta/delta/adcreatives/*/*.parquet',
                DATA_SOURCE = 'sa_esquiregeneral',
                FORMAT = 'PARQUET'
            ) WITH (
                account_id VARCHAR(32),
                actor_id VARCHAR(32),
                applink_treatment VARCHAR(16),
                authorization_category VARCHAR(16),
                auto_update VARCHAR(1),
                body VARCHAR(MAX),
                branded_content_sponsor_page_id VARCHAR(32),
                bundle_folder_id VARCHAR(32),
                call_to_action_type VARCHAR(16),
                categorization_criteria VARCHAR(16),
                category_media_source VARCHAR(16),
                collaborative_ads_lsb_image_bank_id VARCHAR(32),
                destination_set_id VARCHAR(32),
                dynamic_ad_voice VARCHAR(16),
                effective_authorization_category VARCHAR(32),
                effective_instagram_media_id VARCHAR(32),
                effective_instagram_story_id VARCHAR(32),
                effective_object_story_id VARCHAR(128),
                enable_direct_install VARCHAR(1),
                id VARCHAR(32),
                image_file VARCHAR(128),
                image_hash VARCHAR(32),
                image_url VARCHAR(1024),
                is_dco_internal VARCHAR(1),
                link_deep_link_url VARCHAR(1024),
                link_destination_display_url VARCHAR(1024),
                link_og_id VARCHAR(32),
                link_url VARCHAR(1024),
                messenger_sponsored_message VARCHAR(1),
                name VARCHAR(256),
                object_id VARCHAR(32),
                object_store_url VARCHAR(1024),
                object_story_id VARCHAR(32),
                object_type VARCHAR(32),
                object_url VARCHAR(1024),
                place_page_set_id VARCHAR(32),
                playable_asset_id VARCHAR(32),
                product_set_id VARCHAR(32),
                source_instagram_media_id VARCHAR(32),
                status VARCHAR(16),
                template_url VARCHAR(1024),
                thumbnail_id VARCHAR(32),
                thumbnail_url VARCHAR(1024),
                title VARCHAR(128),
                url_tags VARCHAR(1024),
                use_page_actor_override VARCHAR(1),
                video_id VARCHAR(32)
            ) AS [data]
            WHERE data.filepath(1) = (SELECT max_date from latest_date)
        )
        SELECT * FROM new_data
        UNION
        SELECT 
            account_id,
            actor_id,
            applink_treatment,
            authorization_category,
            auto_update,
            body,
            branded_content_sponsor_page_id,
            bundle_folder_id,
            call_to_action_type,
            categorization_criteria,
            category_media_source,
            collaborative_ads_lsb_image_bank_id,
            destination_set_id,
            dynamic_ad_voice,
            effective_authorization_category,
            effective_instagram_media_id,
            effective_instagram_story_id,
            effective_object_story_id,
            enable_direct_install,
            id,
            image_file,
            image_hash,
            image_url,
            is_dco_internal,
            link_deep_link_url,
            link_destination_display_url,
            link_og_id,
            link_url,
            messenger_sponsored_message,
            name,
            object_id,
            object_store_url,
            object_story_id,
            object_type,
            object_url,
            place_page_set_id,
            playable_asset_id,
            product_set_id,
            source_instagram_media_id,
            status,
            template_url,
            thumbnail_id,
            thumbnail_url,
            title,
            url_tags,
            use_page_actor_override,
            video_id
        FROM dashboard.adcreatives
        WHERE NOT EXISTS (
            SELECT 1
            FROM new_data
            WHERE new_data.id = dashboard.adcreatives.id
        )
    """,
}
