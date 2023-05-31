from libs.utils.geometry import wkb2geojson, geojson2wkb
from marshmallow.fields import Nested
from marshmallow_geojson import GeoJSONSchema
from marshmallow_sqlalchemy.convert import ModelConverter
from sqlalchemy.dialects.mssql.base import ischema_names as mssql_ischema_names
import geoalchemy2.types


class GeometryJSON(geoalchemy2.types.Geometry):
    """
    Custom SQLAlchemy Geometry type that handles conversion between WKB and GeoJSON.
    """

    def result_processor(self, dialect, coltype):
        """
        Return the result processing function for the GeometryJSON type.

        Parameters
        ----------
        dialect : sqlalchemy.engine.interfaces.Dialect
            The SQLAlchemy dialect.
        coltype : sqlalchemy.sql.type_api.TypeEngine
            The column type.

        Returns
        -------
        Callable[[Any], Any]
            The result processing function.

        """

        def process(value):
            return wkb2geojson(value) if value else value

        return process

    def bind_processor(self, dialect):
        """
        Return the bind processing function for the GeometryJSON type.

        Parameters
        ----------
        dialect : sqlalchemy.engine.interfaces.Dialect
            The SQLAlchemy dialect.

        Returns
        -------
        Callable[[Any], Any]
            The bind processing function.

        """

        def process(value):
            return geojson2wkb(value) if value else value

        return process


class GeographyJSON(GeometryJSON):
    """
    Custom SQLAlchemy Geography type that extends the GeometryJSON type.
    """


mssql_ischema_names["geometry"] = GeometryJSON
mssql_ischema_names["geography"] = GeographyJSON


class GeoJSONField(Nested):
    """
    Custom Marshmallow field for GeoJSON data.
    """

    def __init__(self, **kwargs):
        super().__init__(GeoJSONSchema, **kwargs)


ModelConverter.SQLA_TYPE_MAPPING[GeometryJSON] = GeoJSONField
ModelConverter.SQLA_TYPE_MAPPING[GeographyJSON] = GeoJSONField
