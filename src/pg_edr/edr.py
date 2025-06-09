# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import logging

from geoalchemy2 import Geometry  # noqa - this isn't used explicitly but is needed to process Geometry columns
from geoalchemy2.functions import ST_MakeEnvelope
from geoalchemy2.shape import to_shape
import shapely
from typing import Optional

from sqlalchemy import func, case, select
from sqlalchemy.orm import Session, relationship, aliased
from sqlalchemy.sql.expression import or_, and_

from pygeoapi.provider.base_edr import BaseEDRProvider
from pygeoapi.provider.sql import GenericSQLProvider

from pg_edr.lib import get_base_schema
from pg_edr.lib import get_column_from_qualified_name as gqname

LOGGER = logging.getLogger(__name__)


GEOGRAPHIC_CRS = {
    "coordinates": ["x", "y"],
    "system": {
        "type": "GeographicCRS",
        "id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
    },
}

TEMPORAL_RS = {
    "coordinates": ["t"],
    "system": {"type": "TemporalRS", "calendar": "Gregorian"},
}


class EDRProvider(BaseEDRProvider, GenericSQLProvider):
    """
    Generic provider for SQL EDR based on psycopg2
    using sync approach and server side
    cursor (using support class DatabaseCursor)
    """

    def __init__(
        self,
        provider_def: dict,
        driver_name: str,
        extra_conn_args: Optional[dict],
    ):
        """
        GenericSQLProvider Class constructor

        :param provider_def: provider definitions from yml pygeoapi-config.
                             data,id_field, name set in parent class
                             data contains the connection information
                             for class DatabaseCursor
        :param driver_name: database driver name
        :param extra_conn_args: additional custom connection arguments to
                                pass for a query


        :returns: pygeoapi_sql_edr.edr.EDRProvider
        """
        LOGGER.debug("Initialising EDR SQL provider.")
        # Flatten EDR fields
        provider_def.update(**provider_def.get("edr_fields", {}))

        BaseEDRProvider.__init__(self, provider_def)
        GenericSQLProvider.__init__(
            self, provider_def, driver_name, extra_conn_args
        )

        LOGGER.debug("Adding external tables")
        self.external_tables = provider_def.get("external_tables", {})
        external_tables = [
            table.split(".")[-1] for table in self.external_tables
        ]
        self.table_models = (self.table, *external_tables)

        self.base = get_base_schema(
            self.table_models, self.db_search_path[0], self._engine
        )
        self.joins = self._get_relationships()
        self.model = self.base.classes[self.table]

        self.tc = gqname(self.model, self.time_field)
        self.gc = gqname(self.model, self.geom)

        self.parameter_id = provider_def.get("parameter_id", "parameter_id")
        self.pic = gqname(self.model, self.parameter_id)

        self.parameter_name = provider_def.get(
            "parameter_name", "parameter_name"
        )
        self.pnc = gqname(self.model, self.parameter_name)

        self.parameter_unit = provider_def.get(
            "parameter_unit", "parameter_unit"
        )
        self.puc = gqname(self.model, self.parameter_unit)

        self.result_field = provider_def.get("result_field", "value")
        self.rc = gqname(self.model, self.result_field)

        self.location_field = provider_def.get(
            "location_field", "monitoring_location_id"
        )
        self.lc = gqname(self.model, self.location_field)

        self.get_fields()

    def get_fields(self):
        """
        Return fields (columns) from SQL table

        :returns: dict of fields
        """

        LOGGER.debug("Get available fields/properties")

        if not self._fields and hasattr(self, "parameter_id"):
            query = select(self.pic, self.pnc, self.puc).with_joins(self.joins)

            with Session(self._engine) as session:
                for pid, pname, punit in session.execute(query):
                    self._fields[pid] = {
                        "type": "number",
                        "title": pname,
                        "x-ogc-unit": punit,
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
        limit: int = 100,
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

        if location_id:
            return self.location(
                location_id, select_properties, datetime_, limit
            )

        bbox_filter = self._get_bbox_filter(bbox)
        time_filter = self._get_datetime_filter(datetime_)
        parameter_filters = self._get_parameter_filters(select_properties)

        with Session(self._engine) as session:
            parameter_query = (
                select(self.pic)
                .select_from(self.model)
                .with_joins(self.joins)
                .where(bbox_filter)
                .where(parameter_filters)
                .where(time_filter)
            )

            parameters = self._get_parameters(
                [p for (p,) in session.execute(parameter_query)], aslist=True
            )

            LOGGER.debug("Preparing response")
            response = {
                "type": "FeatureCollection",
                "features": [],
                "parameters": parameters,
                "numberReturned": 0,
            }

            location_query = (
                select(self.lc, self.gc)
                .select_from(self.model)
                .with_joins(self.joins)
                .distinct(self.lc)
                .where(bbox_filter)
                .where(parameter_filters)
                .where(time_filter)
                .limit(limit)
            )

            for id, geom in session.execute(location_query):
                response["numberReturned"] += 1
                response["features"].append(
                    self._sqlalchemy_to_feature(id, geom)
                )

        return response

    def location(
        self,
        location_id: str,
        select_properties: list = [],
        datetime_: str = None,
        limit: int = 100,
        **kwargs,
    ):
        coverage = {
            "type": "Coverage",
            "domain": {
                "type": "Domain",
                "domainType": "",
                "axes": {"t": {"values": []}},
                "referencing": [GEOGRAPHIC_CRS, TEMPORAL_RS],
            },
            "parameters": [],
            "ranges": {},
        }

        parameter_filters = self._get_parameter_filters(select_properties)
        time_filter = self._get_datetime_filter(datetime_)
        with Session(self._engine) as session:
            location_query = (
                select(self.gc)
                .select_from(self.model)
                .with_joins(self.joins, isouter=True)
                .where(self.lc == location_id)
            )

            geom = session.execute(location_query).scalar()

            try:
                geom = to_shape(geom)
            except TypeError:
                geom = shapely.geometry.shape(geom)

            coverage["domain"]["domainType"] = geom.geom_type
            if geom.geom_type == "Point":
                coverage["domain"]["axes"].update(
                    {
                        "x": {"values": [geom.x]},
                        "y": {"values": [geom.y]},
                    }
                )
            else:
                coverage["domain"]["axes"]["composite"] = {
                    "dataType": "polygon",
                    "coordinates": ["x", "y"],
                    "values": shapely.geometry.mapping(geom),
                }

            properties_query = (
                select(self.pic)
                .select_from(self.model)
                .with_joins(self.joins)
                .where(self.lc == location_id)
                .where(parameter_filters)
                .where(time_filter)
            )

            parameters = {p for (p,) in session.execute(properties_query)}

            coverage["parameters"] = self._get_parameters(parameters)
            for p in parameters:
                coverage["ranges"][str(p)] = {
                    "type": "NdArray",
                    "dataType": "float",
                    "axisNames": ["t"],
                    "shape": [0],
                    "values": [],
                }

            time_query = (
                select(self.tc)
                .select_from(self.model)
                .with_joins(self.joins, isouter=True)
                .where(self.lc == location_id)
                .where(parameter_filters)
                .where(time_filter)
                .order_by(self.tc.desc())
                .distinct()
                .limit(limit)
                .subquery()
            )
            time_subquery = aliased(self.model, time_query)
            time_alias = getattr(time_subquery, self.time_field)

            # Create select columns for each parameter
            select_columns = [
                case(
                    (
                        and_(parameter == self.pic, location_id == self.lc),
                        self.rc,
                    ),
                    else_=None,
                ).label(str(parameter))
                for parameter in parameters
            ]

            # Construct the query
            results = (
                session.query(time_alias, *select_columns)
                .select_from(time_subquery)
                .outerjoin(
                    self.model,
                    and_(
                        time_alias == self.tc,
                        location_id == self.lc,
                    ),
                )
            )

            t_values = coverage["domain"]["axes"]["t"]["values"]
            for row in results.limit(limit):
                row = row._asdict()
                t_values.append(row.pop(self.time_field))
                for pname, value in row.items():
                    parameter = coverage["ranges"][pname]
                    parameter["values"].append(value)
                    parameter["shape"][0] += 1

        if len(t_values) > 1:
            coverage["domain"]["domainType"] += "Series"

        return coverage

    def _sqlalchemy_to_feature(self, id, wkb_geom, crs_transform_out=None):
        feature = {
            "type": "Feature",
            "id": id,
        }

        # Convert geometry to GeoJSON style
        try:
            shapely_geom = to_shape(wkb_geom)
        except TypeError:
            shapely_geom = shapely.geometry.shape(wkb_geom)
        if crs_transform_out is not None:
            shapely_geom = crs_transform_out(shapely_geom)
        geojson_geom = shapely.geometry.mapping(shapely_geom)
        feature["geometry"] = geojson_geom

        return feature

    def _get_parameter_filters(self, parameter):
        if not parameter:
            return True  # Let everything through

        # Convert parameter filters into SQL Alchemy filters
        filter_group = [self.pic == value for value in parameter]
        return or_(*filter_group)

    def _get_parameters(self, parameters: set = {}, aslist=False):
        """
        Generate parameters

        :param parameters: The datastream data to generate parameters for.
        :param aslist: The label for the parameter.

        :returns: A dictionary containing the parameter definition.
        """
        if not parameters:
            parameters = self.fields.keys()

        out_params = {}
        for param in set(parameters):
            conf_ = self.fields[param]
            out_params[param] = {
                "id": param,
                "type": "Parameter",
                "name": conf_["title"],
                "observedProperty": {
                    "id": param,
                    "label": {"en": conf_["title"]},
                },
                "unit": {
                    "label": {"en": conf_["title"]},
                    "symbol": {
                        "value": conf_["x-ogc-unit"],
                        "type": "http://www.opengis.net/def/uom/UCUM/",
                    },
                },
            }

        return list(out_params.values()) if aslist else out_params

    def _get_relationships(self):
        allowed = list()
        for ext_table, rel in self.external_tables.items():
            if "." in ext_table:
                parent, ext_table = ext_table.split(".", 1)
                left, right = parent, ext_table
            else:
                left, right = self.table, ext_table

            table_model = self.base.classes[left]
            ext_table_model = self.base.classes[right]

            foreign_key = gqname(table_model, rel["foreign"])
            remote_key = gqname(ext_table_model, rel["remote"])
            allowed.append((ext_table_model, foreign_key == remote_key))

            if hasattr(table_model, ext_table):
                LOGGER.debug(f"{left} has existing relationship to {right}")
                continue

            ext_relationship = relationship(
                ext_table_model,
                primaryjoin=foreign_key == remote_key,
                foreign_keys=[foreign_key],
                viewonly=True,
            )
            setattr(table_model, ext_table, ext_relationship)

        return tuple(allowed)

    def __repr__(self):
        return f"<EDRProvider> {self.table}"


class PostgresEDRProvider(EDRProvider):
    """
    A provider for querying a PostgreSQL database
    """

    def __init__(self, provider_def: dict):
        """
        PostgreSQLProvider Class constructor

        :param provider_def: provider definitions from yml pygeoapi-config.
                             data,id_field, name set in parent class
                             data contains the connection information
                             for class DatabaseCursor
        :returns: pygeoapi.provider.sql.PostgreSQLProvider
        """

        driver_name = "postgresql+psycopg2"
        extra_conn_args = {
            "client_encoding": "utf8",
            "application_name": "pygeoapi",
        }
        super().__init__(provider_def, driver_name, extra_conn_args)

    def _get_bbox_filter(self, bbox: list[float]):
        """
        Construct the bounding box filter function
        """
        if not bbox:
            return True  # Let everything through if no bbox

        # Since this provider uses postgis, we can use ST_MakeEnvelope
        envelope = ST_MakeEnvelope(*bbox)
        bbox_filter = self.gc.intersects(envelope)

        return bbox_filter


class MySQLEDRProvider(EDRProvider):
    """
    A provider for a MySQL EDR
    """

    def __init__(self, provider_def: dict):
        """
        MySQLProvider Class constructor

        :param provider_def: provider definitions from yml pygeoapi-config.
                             data,id_field, name set in parent class
                             data contains the connection information
                             for class DatabaseCursor
        :returns: pygeoapi.provider.sql.MySQLProvider
        """

        driver_name = "mysql+pymysql"
        extra_conn_args = {"charset": "utf8mb4"}
        super().__init__(provider_def, driver_name, extra_conn_args)

    def _get_bbox_filter(self, bbox: list[float]):
        """
        Construct the bounding box filter function
        """
        if not bbox:
            return True  # Let everything through if no bbox

        # If we are using mysql we can't use ST_MakeEnvelope since it is
        # postgis specific and thus we have to use MBRContains with a WKT
        # POLYGON

        # Create WKT POLYGON from bbox: (minx, miny, maxx, maxy)
        minx, miny, maxx, maxy = bbox
        polygon_wkt = f"POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))"  # noqa
        # Use MySQL MBRContains for index-accelerated bounding box checks
        bbox_filter = func.MBRContains(
            func.ST_GeomFromText(polygon_wkt), self.gc
        )
        return bbox_filter
