# from ...azure.sql import AzSQLEngine
# import pandas as pd
# import os
# import json

# from smartystreets_python_sdk import StaticCredentials, exceptions, ClientBuilder
# from smartystreets_python_sdk.us_street import Lookup as StreetLookup

# return_cols = f"""
#     [Zipcode]
#     ,[City]
#     ,[State]
#     ,[Population]
#     ,[Density]
#     ,[Military]
#     ,[AgeMedian]
#     ,[Male]
#     ,[Female]
#     ,[Married]
#     ,[FamilySize]
#     ,[IncomeHouseholdMedian]
#     ,[IncomeHouseholdSixFigure]
#     ,[HomeOwnership]
#     ,[HomeValue]
#     ,[RentMedian]
#     ,[EducationCollegeOrAbove]
#     ,[LaborForceParticipation]
#     ,[UnemploymentRate]
#     ,[RaceWhite]
#     ,[RaceBlack]
#     ,[RaceAsian]
#     ,[RaceNative]
#     ,[RacePacific]
#     ,[RaceOther]
#     ,[RaceMultiple]
#     ,[LatLong].Lat AS [Latitude]
#     ,[LatLong].Long AS [Longitude]
#     ,dbo.geo2json([Boundary]) AS [GeoJSON]
# """

# class ZipcodeEngine:
#     def _get(self, sql_query:str):
#         sql_conn = AzSQLEngine("legacy", "universal")
#         data = pd.read_sql(sql_query, sql_conn)
#         if 'GeoJSON' in data.columns:
#             data['GeoJSON'] = data['GeoJSON'].apply(json.loads)
#         return json.loads(data.to_json(orient='records'))

#     def get_list(self, zip_list):
#         return self._get(f"""
#             SELECT
#                 {return_cols}
#             FROM [dbo].[Zipcodes]
#             WHERE [Zipcode] IN ('{"','".join(zip_list)}')
#         """)
        
#     # return all requested zipcodes within a radius (in meters)
#     def get_radius_list(self, lat, lon, radius):
#         return self._get(f"""
#             DECLARE @circle GEOGRAPHY;
#             SET @circle = GEOGRAPHY::Point({lat},{lon}, 4326).STBuffer({radius});
#             WITH ZipPolys AS (
#                 SELECT ZP.*
#                 FROM [dbo].[Zipcode Prefix Polygons] AS ZPP
#                     JOIN [dbo].[Zipcodes] AS ZP
#                     ON ZP.Zipcode LIKE CONCAT(RIGHT('00' + CAST(ZPP.[Prefix] AS VARCHAR(2)),2),'%')
#                 WHERE 
#                     @circle.STWithin(ZPP.[Boundary]) = 0
#             )
#             SELECT
#                 {return_cols}
#             FROM ZipPolys
#             WHERE
#                 [Boundary].STWithin(@circle) = 1
#                 OR
#                 @circle.STOverlaps([Boundary]) = 1
#                 OR 
#                 @circle.STWithin([Boundary]) = 1
#         """)

#     def get_list_by_state(self, state):
#         return self._get(f"""
#             SELECT
#                 {return_cols}
#             FROM [dbo].[Zipcodes]
#             WHERE [State] = '{state}' 
#         """)

#     def get_list_by_citystate(self, city, state):
#         return self._get(f"""
#             SELECT
#                 {return_cols}
#             FROM [dbo].[Zipcodes]
#             WHERE [State] = '{state}' AND [City] = '{city}'
#         """)

#     def get_point_by_address(self, address):
#         credentials = StaticCredentials(os.environ['smarty_streets_id'], os.environ['smarty_streets_token'])
#         client = ClientBuilder(credentials).with_licenses(["us-core-enterprise-cloud"]).build_us_street_api_client()

#         lookup = StreetLookup()
#         lookup.street = address
#         try:
#             client.send_lookup(lookup)
#         except exceptions.SmartyException as err:
#             print(err)
#             return

#         return lookup.result[0].metadata.latitude, lookup.result[0].metadata.longitude