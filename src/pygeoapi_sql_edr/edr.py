# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import logging

from datetime import datetime
from decimal import Decimal

from geoalchemy2 import Geometry  # noqa - this isn't used explicitly but is needed to process Geometry columns
from geoalchemy2.shape import to_shape
import shapely

from sqlalchemy import func
from sqlalchemy.orm import foreign, remote, Session, relationship
from sqlalchemy.sql.expression import or_

from pygeoapi.provider.postgresql import PostgreSQLProvider, get_table_model
from pygeoapi.provider.base_edr import BaseEDRProvider

from pygeoapi_sql_edr.lib import get_column_from_qualified_name as gqname
from pygeoapi_sql_edr.lib import recursive_getattr as rgetattr

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


class EDRProvider(BaseEDRProvider, PostgreSQLProvider):
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
        self.joins = list()
        self.external_tables = provider_def.get("external_tables", {})
        for ext_table, ext_config in self.external_tables.items():
            ext_table_model = get_table_model(
                ext_table,
                ext_config["remote"],
                self.db_search_path,
                self._engine,
            )
            self.table_models.append(ext_table_model)

            foreign_key = foreign(
                getattr(self.table_model, ext_config["foreign"])
            )
            remote_key = remote(getattr(ext_table_model, ext_config["remote"]))
            self.joins.append((ext_table_model, foreign_key == remote_key))
            foreign_relationship = relationship(
                ext_table_model,
                primaryjoin=foreign_key == remote_key,
                foreign_keys=[foreign_key],
                uselist=False,
                viewonly=True,
            )
            setattr(self.table_model, ext_table, foreign_relationship)

        LOGGER.debug("Getting EDR Columns")
        edr_fields = provider_def.get("edr_fields", {})

        self.tc = gqname(self.table_model, self.time_field)
        self.gc = gqname(self.table_model, self.geom)

        self.parameter_id = edr_fields.get("parameter_id", "parameter_id")
        self.pic = gqname(self.table_model, self.parameter_id)

        self.parameter_name = edr_fields.get(
            "parameter_name", "parameter_name"
        )  # noqa
        self.pnc = gqname(self.table_model, self.parameter_name)

        self.parameter_unit = edr_fields.get(
            "parameter_unit", "parameter_unit"
        )  # noqa
        self.puc = gqname(self.table_model, self.parameter_unit)

        self.result_field = edr_fields.get("result_field", "value")
        self.rc = gqname(self.table_model, self.result_field)

        self.location_field = edr_fields.get(
            "location_field", "monitoring_location_id"
        )  # noqa
        self.lc = gqname(self.table_model, self.location_field)

        self.get_fields()

    def get_fields(self):
        """
        Return fields (columns) from PostgreSQL table

        :returns: dict of fields
        """

        LOGGER.debug("Get available fields/properties")

        if not self._fields and hasattr(self, "parameter_id"):
            with Session(self._engine) as session:
                result = (
                    session.query(self.table_model)
                    .distinct(self.pic)
                    .with_entities(self.pic, self.pnc, self.puc)
                )

                for j in self.joins:
                    result = result.join(*j)

                for parameter_id, parameter_name, parameter_unit in result:
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
        limit: int = 10,
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

        # if location_id:
        #     return self.single_location(location_id,
        #                                 select_properties, datetime_)

        bbox_filter = self._get_bbox_filter(bbox)
        time_filter = self._get_datetime_filter(datetime_)
        parameter_filters = self._get_parameter_filters(select_properties)

        with Session(self._engine) as session:
            results = (
                session.query(self.table_model)
                .where(bbox_filter)
                .where(parameter_filters)
                .where(time_filter)
            )

            LOGGER.debug("Preparing response")
            response = {
                "type": "FeatureCollection",
                "features": [],
                "numberReturned": 0,
            }
            for item in results.distinct(self.lc).limit(limit):
                response["numberReturned"] += 1
                response["features"].append(
                    self._sqlalchemy_to_feature(
                        item,
                        results.where(
                            self.lc == rgetattr(item, self.location_field)
                        ),
                    )
                )

        return response

    # @BaseEDRProvider.register()
    # def single_location(
    #     self,
    #     location_id: str,
    #     select_properties: list = [],
    #     datetime_: str = None,
    #     **kwargs,
    # ):

    #     parameter_filters = self._get_parameter_filters(select_properties)
    #     time_filter = self._get_datetime_filter(datetime_)

    def _sqlalchemy_to_feature(self, item, results, crs_transform_out=None):
        # Add properties from item

        datetime_view = func.concat(func.min(self.tc), "/", func.max(self.tc))
        datetime_range = results.with_entities(datetime_view).scalar()
        parameters = results.distinct(self.pic).with_entities(self.pic).all()
        parameters = [_[0] for _ in parameters]

        feature = {
            "type": "Feature",
            "id": getattr(item, self.location_field),
            "properties": {
                "datetime": datetime_range,
                "parameter-name": parameters,
            },
        }

        # Convert geometry to GeoJSON style
        if hasattr(item, self.geom):
            wkb_geom = getattr(item, self.geom)
            shapely_geom = to_shape(wkb_geom)
            if crs_transform_out is not None:
                shapely_geom = crs_transform_out(shapely_geom)
            geojson_geom = shapely.geometry.mapping(shapely_geom)
            feature["geometry"] = geojson_geom
        else:
            feature["geometry"] = None

        return feature

    def _get_parameter_filters(self, parameter):
        if not parameter:
            return True  # Let everything through

        # Convert parameter filters into SQL Alchemy filters
        filter_group = [self.pic == value for value in parameter]
        return or_(*filter_group)

    def __repr__(self):
        return f"<EDRProvider> {self.table}"
