import json
import pandas as pd
import os
from libs.data import register_binding
from libs.data import from_bind
from libs.utils.geometry import wkb2geojson

return_cols = f"""
    [Zipcode]
    ,[City]
    ,[State]
    ,[Population]
    ,[Density]
    ,[Military]
    ,[AgeMedian]
    ,[Male]
    ,[Female]
    ,[Married]
    ,[FamilySize]
    ,[IncomeHouseholdMedian]
    ,[IncomeHouseholdSixFigure]
    ,[HomeOwnership]
    ,[HomeValue]
    ,[RentMedian]
    ,[EducationCollegeOrAbove]
    ,[LaborForceParticipation]
    ,[UnemploymentRate]
    ,[RaceWhite]
    ,[RaceBlack]
    ,[RaceAsian]
    ,[RaceNative]
    ,[RacePacific]
    ,[RaceOther]
    ,[RaceMultiple]
    ,[LatLong].Lat AS [Latitude]
    ,[LatLong].Long AS [Longitude]
    ,[Boundary].STAsBinary() AS [GeoJSON]
"""

class ZipcodeEngine:
    def __init__(self):
        """
        Initialize the SQL connection.
        """
        # connect to legacy SQL server
        register_binding(
            "sql_legacy",
            "Structured",
            "sql",
            url=os.environ['DATABIND_SQL_LEGACY']
        )
        self.provider = from_bind("sql_legacy")
    
    def format_output(self, df):
        if 'GeoJSON' in df.columns:
            df['GeoJSON'] = df['GeoJSON'].apply(wkb2geojson)
        return df

    def load_from_list(self, zipcodes:list) -> pd.DataFrame:
        """
        Given a list of zipcodes, return the demographics and geography for those zipcodes.

        Params:
        zipcodes    : List of zipcodes formatted as strings or integers. If a 4-digit zipcode is passed, a leading zero will be inferred.
        """
        # format the zipcode list into a SQL string
        zipcodes_str = ','.join([f"'{('00000'+str(zip))[-5:]}'" for zip in zipcodes])

        # exceute and return the query results
        return self.format_output(pd.read_sql(
            f"""SELECT {return_cols}
            FROM [dbo].[Zipcodes]
            WHERE [Zipcode] IN ({zipcodes_str})
            """,
            self.provider.engine
        ))

    def load_from_point(self, latitude:float, longitude:float, radius:int=1000) -> pd.DataFrame:
        """
        Given a centerpoint and zipcode, return the demographics and geography for zipcodes within that area.

        Params:
        latitude    : Latitude of the centerpoint.
        longitude   : Longitude of the centerpoint.
        radius      : Radius to search, in meters. If none is passed, the default value is 1000 meters.
        """
        # exceute and return the query results
        return self.format_output(pd.read_sql(f"""
            DECLARE @circle GEOGRAPHY;
            SET @circle = GEOGRAPHY::Point({latitude},{longitude}, 4326).STBuffer({radius});
            WITH ZipPolys AS (
                SELECT ZP.*
                FROM [dbo].[Zipcode Prefix Polygons] AS ZPP
                    JOIN [dbo].[Zipcodes] AS ZP
                    ON ZP.Zipcode LIKE CONCAT(ZPP.Prefix,'%')
                WHERE 
                    @circle.STWithin(ZPP.[Boundary]) = 0
            )
            SELECT
                {return_cols}
            FROM ZipPolys
            WHERE
                [Boundary].STWithin(@circle) = 1
                OR
                @circle.STOverlaps([Boundary]) = 1
            """,
            self.provider.engine
        ))
    
    def load_by_attribute(self, city:str=None, state:str=None) -> pd.DataFrame:
        """
        Given a city and/or state, return the demographics and geography for zipcodes within that city/state.

        Params:
        city    : US city name.
        state   : US state two-letter abbreviation.
        """
        # base query
        query = f"""
        SELECT
            {return_cols}
        FROM Zipcodes
        """
        # add applicable city/state filters
        if city and not state:
            query += f"WHERE [City] = '{city}'"
        elif state and not city:
            query += f"WHERE [State] = '{state}'"
        elif city and state:
            query += f"WHERE [City] = '{city}' AND [State] = '{state}'"
        else:
            raise Exception("Must pass either a city or state to load by attribute.")

        # exceute and return the query results
        return self.format_output(pd.read_sql(
            query,
            self.provider.engine
        ))