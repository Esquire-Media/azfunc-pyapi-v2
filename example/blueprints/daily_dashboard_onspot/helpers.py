# File: example/blueprints/daily_dashboard_onspot/helpers.py

def cetas_query_observations(instance_id):
    return f"""
        WITH [locations] AS (
            SELECT 
                *
            FROM OPENROWSET(
                BULK 'dashboard/raw/{instance_id}/locations.csv',
                DATA_SOURCE = 'sa_esquireonspot',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                HEADER_ROW = TRUE
            ) AS [data]
        )
        SELECT
            [locations].[location_id],
            [observations].*
        FROM (
            SELECT 
                CONCAT(
                    LEFT(data.filepath(1),2), 
                    '~', 
                    RIGHT(data.filepath(1),LEN(data.filepath(1))-2)
                ) AS [esq_id],
                [deviceid] AS [did],
                [lat] AS [latitude],
                [lng] AS [longitude],
                CAST(DATEADD(second, [timestamp]/1000, {{d '1970-01-01'}}) AS DATETIME) AS [start]
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
        ) AS [observations]
        JOIN [locations]
            ON [locations].[esq_id] = [observations].[esq_id]
    """

def cetas_query_unique_deviceids(instance_id):
    return f"""
        SELECT DISTINCT [did]
        FROM OPENROWSET(
            BULK 'dashboard/cetas/{instance_id}/observations/*.parquet',
            DATA_SOURCE = 'sa_esquireonspot',
            FORMAT = 'PARQUET'
        ) AS [data]
    """

def cetas_query_demographics(instance_id):
    return f"""
        SELECT
            ROW_NUMBER() OVER(
                PARTITION BY [did] 
                ORDER BY [did] 
                DESC
            ) AS [rank],
            * 
        FROM OPENROWSET(
            BULK 'dashboard/raw/{instance_id}/demographics/*',
            DATA_SOURCE = 'sa_esquireonspot',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0',
            HEADER_ROW = TRUE
        ) WITH (
            [location (venue)] VARCHAR(32),
            [state] NCHAR(2),
            [city] VARCHAR(32),
            [zip] NCHAR(5),
            [zip4] NCHAR(4),
            [did] VARCHAR(64),
            [number of instances (count seen at location)] INT,
            [cbmsa_name] VARCHAR(64),
            [msa_name] VARCHAR(64),
            [fips_name] VARCHAR(32),
            [male] BIT,
            [female] BIT,
            [married] BIT,
            [dwelling_type apt/multi-family] BIT,
            [dwelling_type single family] BIT,
            [completed high school] BIT,
            [some college] BIT,
            [completed college] BIT,
            [completed graduate school] BIT,
            [attended vocational/technical school] BIT,
            [household_income under $15000] BIT,
            [household_income $15000 - $24999] BIT,
            [household_income $25000 - $34999] BIT,
            [household_income $35000 - $49999] BIT,
            [household_income $50000 - $74999] BIT,
            [household_income $75000 - $99999] BIT,
            [household_income $100000 - $149999] BIT,
            [household_income $150000 - $159999] BIT,
            [household_income $160000 - $164999] BIT,
            [household_income $165000 - $199999] BIT,
            [household_income $200000 - $249999] BIT,
            [household_income $250000+] BIT,
            [household_size] INT,
            [home_owner] BIT,
            [networth less than $50000] BIT,
            [networth $50000 - $99999] BIT,
            [networth $100000 - $249999] BIT,
            [networth $250000 - $499999] BIT,
            [networth $500000+] BIT,
            [presence_of_children] BIT,
            [presence_of_single_adult] BIT,
            [presence_of_young_adult] BIT,
            [presence_of_senior_adult] BIT,
            [presence_of_veteran] BIT,
            [estimated_age 18-24] BIT,
            [estimated_age 25-29] BIT,
            [estimated_age 30-34] BIT,
            [estimated_age 35-39] BIT,
            [estimated_age 40-44] BIT,
            [estimated_age 45-49] BIT,
            [estimated_age 50-54] BIT,
            [estimated_age 55-59] BIT,
            [estimated_age 60-64] BIT,
            [estimated_age 65-69] BIT,
            [estimated_age 70+] BIT,
            [buyer_children_apparel] BIT,
            [buyer_men_apparel] BIT,
            [buyer_big_and_tall_men_apparel] BIT,
            [buyer_women_apparel] BIT,
            [buyer_petite_women_apparel] BIT,
            [buyer_plus_women_apparel] BIT,
            [buyer_young_men_apparel] BIT,
            [buyer_young_women_apparel] BIT,
            [buyer_audio_books_and_music] BIT,
            [buyer_auto] BIT,
            [buyer_cosmetic_products] BIT,
            [buyer_books_and_magazines] BIT,
            [buyer_children_products] BIT,
            [buyer_gardening_products] BIT,
            [buyer_health_and_beauty_products] BIT,
            [buyer_high_end_appliances] BIT,
            [buyer_hunting_and_shooting] BIT,
            [buyer_jewelry] BIT,
            [buyer_lifestyles] BIT,
            [buyer_luggage] BIT,
            [buyer_mail_orders] BIT,
            [buyer_mail_order_responder] BIT,
            [buyer_online_products] BIT,
            [buyer_photography] BIT,
            [buyer_religious_material] BIT,
            [buyer_sports] BIT,
            [buyer_value_priced_merchandise] BIT,
            [interest_arts] BIT,
            [interest_arts_antiques] BIT,
            [interest_auto] BIT,
            [interest_aviation] BIT,
            [interest_collector] BIT,
            [interest_board_games] BIT,
            [interest_sailing] BIT,
            [interest_camping] BIT,
            [interest_career] BIT,
            [interest_cat] BIT,
            [interest_children] BIT,
            [interest_antiques] BIT,
            [interest_coins] BIT,
            [interest_collectibles] BIT,
            [interest_lifestyles] BIT,
            [interest_military] BIT,
            [interest_sports] BIT,
            [interest_stamps] BIT,
            [interest_art_collectibles] BIT,
            [interest_computing_home_office] BIT,
            [interest_cooking] BIT,
            [interest_gourmet_cooking] BIT,
            [interest_crafts] BIT,
            [interest_cruise] BIT,
            [interest_diet] BIT,
            [interest_dog] BIT,
            [interest_domestic_travel] BIT,
            [interest_education] BIT,
            [interest_aerobic] BIT,
            [interest_running] BIT,
            [interest_walking] BIT,
            [interest_fishing] BIT,
            [interest_food_wines] BIT,
            [interest_gambling] BIT,
            [interest_golf] BIT,
            [interest_health_medical] BIT,
            [interest_home_gardening] BIT,
            [interest_home_furnishings] BIT,
            [interest_home_improvement] BIT,
            [interest_diy] BIT,
            [interest_house_plants] BIT,
            [interest_hunting] BIT,
            [interest_career_improvement] BIT,
            [interest_christian_family] BIT,
            [interest_grandchildren] BIT,
            [interest_parenting] BIT,
            [interest_pets] BIT,
            [interest_international_travel] BIT,
            [interest_equestrian] BIT,
            [interest_knitting] BIT,
            [interest_motorcycling] BIT,
            [interest_musical_instruments] BIT,
            [interest_nascar] BIT,
            [interest_natural_foods] BIT,
            [interest_other_pet] BIT,
            [interest_photography] BIT,
            [interest_scuba_diving] BIT,
            [interest_self_improvement] BIT,
            [interest_snow_skiing] BIT,
            [interest_sweepstakes] BIT,
            [interest_tennis] BIT,
            [interest_travel] BIT,
            [interest_woodworking] BIT,
            [interest_working_women] BIT,
            [entertain_gaming] BIT,
            [entertain_dvds] BIT,
            [entertain_tv_movies] BIT,
            [entertain_computers] BIT,
            [entertain_pc_games] BIT,
            [entertain_music_home_stereo] BIT,
            [entertain_music_player] BIT,
            [entertain_music_listener] BIT,
            [entertain_movies] BIT,
            [entertain_tv] BIT,
            [entertain_videos_games] BIT,
            [entertain_satellite] BIT,
            [entertain_theater] BIT,
            [entertain_music] BIT,
            [donor_contribution] BIT,
            [donor_mail_order] BIT,
            [donor_charitable] BIT,
            [donor_animal] BIT,
            [donor_arts] BIT,
            [donor_children] BIT,
            [donor_wildlife] BIT,
            [donor_environment] BIT,
            [donor_health] BIT,
            [donor_international_aid] BIT,
            [donor_political] BIT,
            [donor_political_conser] BIT,
            [donor_political_liberal] BIT,
            [donor_community] BIT,
            [donor_religious] BIT,
            [donor_veteran] BIT,
            [donor_other] BIT,
            [spectator_auto] BIT,
            [spectator_sports_tv] BIT,
            [spectator_football] BIT,
            [spectator_baseball] BIT,
            [spectator_basketball] BIT,
            [spectator_hockey] BIT,
            [spectator_soccer] BIT
        ) AS [data]
    """

def cetas_query_sisense(instance_id):
    return f"""
        WITH [demographics] AS (
            SELECT 
                [did],
                [zip]
            FROM OPENROWSET(
                BULK 'dashboard/cetas/{instance_id}/demographics/*.parquet',
                DATA_SOURCE = 'sa_esquireonspot',
                FORMAT = 'PARQUET'
            ) AS [data]
        )
        SELECT DISTINCT *
        FROM (
            SELECT DISTINCT
                [O].*,
                [Z].[zip]
            FROM (
                SELECT
                    [location_id],
                    [esq_id],
                    [did],
                    CAST([start] AS DATE) AS [date],
                    DATEPART(HOUR, [start]) AS [hour],
                    DATEADD(
                        HOUR,
                        DATEPART(HOUR,[start]),
                        CAST(CAST([start] AS DATE) AS DATETIME2)
                    ) AS [timestamp],
                    AVG([latitude]) AS [latitude],
                    AVG([longitude]) AS [longitude]
                FROM OPENROWSET(
                    BULK 'dashboard/cetas/{instance_id}/observations/*.parquet',
                    DATA_SOURCE = 'sa_esquireonspot',
                    FORMAT = 'PARQUET'
                ) AS [data]
                GROUP BY 
                    [location_id],
                    [esq_id],
                    [did],
                    CAST([start] AS DATE),
                    DATEPART(HOUR, [start])
            ) AS [O]
            LEFT JOIN [demographics] AS [Z]
                ON [O].[did] = [Z].[did]
        ) AS [data]
    """