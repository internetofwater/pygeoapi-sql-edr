# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import logging

from datetime import datetime
from decimal import Decimal

from geoalchemy2 import Geometry  # noqa - this isn't used explicitly but is needed to process Geometry columns
from geoalchemy2.shape import to_shape
import shapely

from sqlalchemy.orm import foreign, remote, Session, relationship

from pygeoapi.provider.postgresql import PostgreSQLProvider, get_table_model
from pygeoapi.provider.base_edr import BaseEDRProvider

from pygeoapi_sql_edr.lib import get_column_from_qualified_name, recursive_getattr

LOGGER = logging.getLogger(__name__)


# sql-schema only allows these types, so we need to map from sqlalchemy
# string, number, integer, object, array, boolean, null,
# https://json-schema.org/understanding-json-schema/reference/type.html
COLUMN_TYPE_MAP = {
    bool: "boolean",
    datetime: "string",
    Decimal: "number",
    float: "number",
    int: "integer",
    str: "string",
}
DEFAULT_TYPE = "string"

# https://json-schema.org/understanding-json-schema/reference/string#built-in-formats  # noqa
COLUMN_FORMAT_MAP = {
    "date": "date",
    "interval": "duration",
    "time": "time",
    "timestamp": "date-time",
}


class EDRProvider(PostgreSQLProvider, BaseEDRProvider):
    """Generic provider for SQL EDR based on psycopg2
    using sync approach and server side
    cursor (using support class DatabaseCursor)
    """

    def __init__(self, provider_def):
        """
        PostgreSQLProvider Class constructor

        :param provider_def: provider definitions from yml pygeoapi-config.
                             data,id_field, name set in parent class
                             data contains the connection information
                             for class DatabaseCursor

        :returns: pygeoapi_sql_edr.edr.EDRProvider
        """
        LOGGER.debug("Initialising Pseudo-count PostgreSQL provider.")
        BaseEDRProvider.__init__(self, provider_def)
        PostgreSQLProvider.__init__(self, provider_def)

        LOGGER.debug("Adding external tables")
        self.table_models = [self.table_model]
        self.table_joins = {}
        self.external_tables = provider_def.get("external_tables", {})
        for ext_table, ext_config in self.external_tables.items():
            ext_table_model = get_table_model(
                ext_table, ext_config["remote"], self.db_search_path, self._engine
            )
            self.table_models.append(ext_table_model)

            foreign_key = foreign(getattr(self.table_model, ext_config["foreign"]))
            remote_key = remote(getattr(ext_table_model, ext_config["remote"]))

            foreign_relationship = relationship(
                ext_table_model,
                primaryjoin=foreign_key == remote_key,
                foreign_keys=[foreign_key],
                uselist=False,
                viewonly=True,
            )
            setattr(self.table_model, ext_table, foreign_relationship)

        LOGGER.debug("Setting EDR properties")
        self.parameter_id = provider_def.get("parameter_id", "parameter_id")
        self.parameter_name = provider_def.get("parameter_name", "parameter_name")
        self.parameter_unit = provider_def.get("parameter_unit", "parameter_unit")

        parameters = [self.parameter_id, self.parameter_name, self.parameter_unit]
        self.parameters = [
            get_column_from_qualified_name(self.table_model, p) for p in parameters
        ]

        self._fields = {}
        self.get_fields()

    def get_fields(self):
        """
        Return fields (columns) from PostgreSQL table

        :returns: dict of fields
        """

        LOGGER.debug("Get available fields/properties")

        if not self._fields and hasattr(self, "parameters"):
            with Session(self._engine) as session:
                result = session.query(self.table_model).distinct(self.parameter_id)

                for item in result:
                    parameter_id = recursive_getattr(item, self.parameter_id)
                    parameter_name = recursive_getattr(item, self.parameter_name)
                    parameter_unit = recursive_getattr(item, self.parameter_unit)

                    self._fields[parameter_id] = {
                        "type": "number",
                        "title": parameter_name,
                        "x-ogc-unit": parameter_unit,
                    }

        return self._fields

    @BaseEDRProvider.register()
    def items(self, **kwargs):
        """
        Retrieve a collection of items.

        :param kwargs: Additional parameters for the request.
        :returns: A GeoJSON representation of the items.
        """

        # This method is empty due to the way pygeoapi handles items requests
        # We implement this method inside of the feature provider
        pass

    @BaseEDRProvider.register()
    def locations(
        self,
        select_properties: list = [],
        bbox: list = [],
        datetime_: str = None,
        location_id: str = None,
        **kwargs,
    ):
        """
        Extract and return location data from ObservedProperties.

        :param select_properties: List of properties to include.
        :param bbox: Bounding box geometry for spatial queries.
        :param datetime_: Temporal filter for observations.
        :param location_id: Identifier of the location to filter by.

        :returns: A GeoJSON FeatureCollection of locations.
        """

    def _sqlalchemy_to_feature(self, item, crs_transform_out=None):
        feature = {"type": "Feature"}

        # Add properties from item
        item_dict = item.__dict__
        for ext_table in self.external_tables.keys():
            if ext_table in item_dict:
                LOGGER.debug(f"Removing {ext_table}")
                item_dict.pop(ext_table)

        item_dict.pop("_sa_instance_state")  # Internal SQLAlchemy metadata
        feature["properties"] = item_dict
        feature["id"] = item_dict.pop(self.id_field)

        # Convert geometry to GeoJSON style
        if feature["properties"].get(self.geom):
            wkb_geom = feature["properties"].pop(self.geom)
            shapely_geom = to_shape(wkb_geom)
            if crs_transform_out is not None:
                shapely_geom = crs_transform_out(shapely_geom)
            geojson_geom = shapely.geometry.mapping(shapely_geom)
            feature["geometry"] = geojson_geom
        else:
            feature["geometry"] = None

        return feature

    def __repr__(self):
        return f"<EDRProvider> {self.table}"
