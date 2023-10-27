from sqlalchemy.orm import Session
from sqlalchemy.sql import text

class ZipcodeEngine:
    def __init__(self, provider):
        self.provider = provider

    def load_from_list(self, zipcode_list:list[str]):
        """
        Load zipcode information given a list of zipcodes.

        Params:
        zipcode_list    : a list of 5-digit zipcode strings. Zero padding will be applied for zipcodes with less than 5 characters.
        """
        session:Session = self.provider.connect()
        zipcodes = self.provider.models["dbo"]["Zipcodes"]

        # check that input is a list
        if not isinstance(zipcode_list, list):
            raise Exception(f"Input `zipcode_list` must be type list, not {type(zipcode_list)}")
        
        # format zipcode inputs as zero-padded strings
        zipcode_list = [('00000'+str(z))[-5:] for z in zipcode_list]

        return (
            session
            .query(
                zipcodes.Zipcode,
                zipcodes.City,
                zipcodes.State,
                zipcodes.Population,
                zipcodes.Density,
                zipcodes.Military,
                zipcodes.AgeMedian,
                zipcodes.Male,
                zipcodes.Female,
                zipcodes.Married,
                zipcodes.FamilySize,
                zipcodes.IncomeHouseholdMedian,
                zipcodes.IncomeHouseholdSixFigure,
                zipcodes.HomeOwnership,
                zipcodes.HomeValue,
                zipcodes.RentMedian,
                zipcodes.EducationCollegeOrAbove,
                zipcodes.LaborForceParticipation,
                zipcodes.UnemploymentRate,
                zipcodes.RaceWhite,
                zipcodes.RaceBlack,
                zipcodes.RaceAsian,
                zipcodes.RaceNative,
                zipcodes.RacePacific,
                zipcodes.RaceOther,
                zipcodes.RaceMultiple,
                text("dbo.geo2json([Boundary]) AS [GeoJSON]"),
                text("[LatLong].Lat AS [Latitude]"),
                text("[LatLong].Long AS [Longitude]")
            )
            .filter(zipcodes.Zipcode.in_(zipcode_list))
        ).all()
    
    def load_from_state(self, state:str):
        """
        Load zipcode information for all zipcodes within a given US state.

        Params:
        state   : two-character string state abbreviation (e.g. "NC")
        """
        session:Session = self.provider.connect()
        zipcodes = self.provider.models["dbo"]["Zipcodes"]

        return (
            session
            .query(
                zipcodes.Zipcode,
                zipcodes.City,
                zipcodes.State,
                zipcodes.Population,
                zipcodes.Density,
                zipcodes.Military,
                zipcodes.AgeMedian,
                zipcodes.Male,
                zipcodes.Female,
                zipcodes.Married,
                zipcodes.FamilySize,
                zipcodes.IncomeHouseholdMedian,
                zipcodes.IncomeHouseholdSixFigure,
                zipcodes.HomeOwnership,
                zipcodes.HomeValue,
                zipcodes.RentMedian,
                zipcodes.EducationCollegeOrAbove,
                zipcodes.LaborForceParticipation,
                zipcodes.UnemploymentRate,
                zipcodes.RaceWhite,
                zipcodes.RaceBlack,
                zipcodes.RaceAsian,
                zipcodes.RaceNative,
                zipcodes.RacePacific,
                zipcodes.RaceOther,
                zipcodes.RaceMultiple,
                text("dbo.geo2json([Boundary]) AS [GeoJSON]"),
                text("[LatLong].Lat AS [Latitude]"),
                text("[LatLong].Long AS [Longitude]")
            )
            .filter(zipcodes.State == state)
        ).all()

    def load_from_citystate(self, city:str, state:str):
        """
        Load zipcode information for all zipcodes within a given US city and state.

        Params:
        city    : string city name
        state   : two-character string state abbreviation (e.g. "NC")
        """
        session:Session = self.provider.connect()
        zipcodes = self.provider.models["dbo"]["Zipcodes"]

        return (
            session
            .query(
                zipcodes.Zipcode,
                zipcodes.City,
                zipcodes.State,
                zipcodes.Population,
                zipcodes.Density,
                zipcodes.Military,
                zipcodes.AgeMedian,
                zipcodes.Male,
                zipcodes.Female,
                zipcodes.Married,
                zipcodes.FamilySize,
                zipcodes.IncomeHouseholdMedian,
                zipcodes.IncomeHouseholdSixFigure,
                zipcodes.HomeOwnership,
                zipcodes.HomeValue,
                zipcodes.RentMedian,
                zipcodes.EducationCollegeOrAbove,
                zipcodes.LaborForceParticipation,
                zipcodes.UnemploymentRate,
                zipcodes.RaceWhite,
                zipcodes.RaceBlack,
                zipcodes.RaceAsian,
                zipcodes.RaceNative,
                zipcodes.RacePacific,
                zipcodes.RaceOther,
                zipcodes.RaceMultiple,
                text("dbo.geo2json([Boundary]) AS [GeoJSON]"),
                text("[LatLong].Lat AS [Latitude]"),
                text("[LatLong].Long AS [Longitude]")
            )
            .filter(zipcodes.State == state)
            .filter(zipcodes.City == city)
        ).all()

    def load_from_radius(self, latitude:float, longitude:float, radius:float):
        """
        Load zipcode information for zipcodes within a given point and radius.

        Params:
        latitude    : latitude of the search area centerpoint
        longitude   : longitude of the search area centerpoint
        radius      : radius to search around the centerpoint
        """

        session:Session = self.provider.connect()

        # define a custom query to pass to SQL Alchemy (this query is more performant than a simple select)
        # this uses super-polygons of each 2-digit zipcode prefix as indexes to narrow down zipcodes which need to be checked for intersection with the search area
        # NOTE this query does not conform to the SQLAlchemy best practices and may cause issues in the future
        query = f"""
            DECLARE @circle GEOGRAPHY;
            SET @circle = GEOGRAPHY::Point({latitude},{longitude}, 4326).STBuffer({radius});
            WITH ZipPolys AS (
                SELECT ZP.*
                FROM [dbo].[Zipcode Prefix Polygons] AS ZPP
                    JOIN [dbo].[Zipcodes] AS ZP
                    ON ZP.Zipcode LIKE CONCAT(RIGHT('00' + CAST(ZPP.[Prefix] AS VARCHAR(2)),2),'%')
                WHERE 
                    @circle.STWithin(ZPP.[Boundary]) = 0
            )
            SELECT
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
                ,dbo.geo2json([Boundary]) AS [GeoJSON]
            FROM ZipPolys
            WHERE
                [Boundary].STWithin(@circle) = 1
                OR
                @circle.STOverlaps([Boundary]) = 1
                OR 
                @circle.STWithin([Boundary]) = 1
            """
        
        return session.execute(text(query))