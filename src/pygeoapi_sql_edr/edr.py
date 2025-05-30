# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import logging

from geoalchemy2 import Geometry  # noqa - this isn't used explicitly but is needed to process Geometry columns
from geoalchemy2.functions import ST_MakeEnvelope
from geoalchemy2.shape import to_shape
import shapely
from typing import Optional

from sqlalchemy import func, case
from sqlalchemy.orm import foreign, remote, Session, relationship, aliased
from sqlalchemy.sql.expression import or_, and_

from pygeoapi.provider.sql import get_table_model, GenericSQLProvider
from pygeoapi.provider.base_edr import BaseEDRProvider

from pygeoapi_sql_edr.lib import get_column_from_qualified_name as gqname
from pygeoapi_sql_edr.lib import recursive_getattr as rgetattr

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
        BaseEDRProvider.__init__(self, provider_def)
        GenericSQLProvider.__init__(
            self, provider_def, driver_name, extra_conn_args
        )

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
                rgetattr(self.table_model, ext_config["foreign"])
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
        )
        self.pnc = gqname(self.table_model, self.parameter_name)

        self.parameter_unit = edr_fields.get(
            "parameter_unit", "parameter_unit"
        )
        self.puc = gqname(self.table_model, self.parameter_unit)

        self.result_field = edr_fields.get("result_field", "value")
        self.rc = gqname(self.table_model, self.result_field)

        self.location_field = edr_fields.get(
            "location_field", "monitoring_location_id"
        )
        self.lc = gqname(self.table_model, self.location_field)

        self.get_fields()

    def get_fields(self):
        """
        Return fields (columns) from SQL table

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
            results = (
                session.query(self.table_model)
                .where(bbox_filter)
                .where(parameter_filters)
                .where(time_filter)
            )

            for j in self.joins:
                results = results.join(*j)

            _ = results.distinct(self.pic).with_entities(self.pic)
            parameters = self._get_parameters(
                [p for (p,) in _.all()], aslist=True
            )

            LOGGER.debug("Preparing response")
            response = {
                "type": "FeatureCollection",
                "features": [],
                "parameters": parameters,
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
            (geom,) = (
                session.query(self.table_model)
                .where(self.lc == location_id)
                .with_entities(self.gc)
                .first()
            )
            geom = to_shape(geom)
            coverage["domain"]["domainType"] = geom.geom_type
            if geom.geom_type == "Point":
                coverage["domain"]["axes"].update(
                    {
                        "x": {"values": [shapely.get_x(geom)]},
                        "y": {"values": [shapely.get_y(geom)]},
                    }
                )
            else:
                coverage["domain"]["axes"]["composite"] = {
                    "dataType": "polygon",
                    "coordinates": ["x", "y"],
                    "values": shapely.geometry.mapping(geom),
                }

            query = (
                session.query(self.tc)
                .where(self.lc == location_id)
                .where(parameter_filters)
                .where(time_filter)
            )

            for j in self.joins:
                query = query.join(*j)

            _ = query.distinct(self.pic).with_entities(self.pic)
            parameters = [p for (p,) in _.all()]

            coverage["parameters"] = self._get_parameters(parameters)
            for p in parameters:
                coverage["ranges"][p] = {
                    "type": "NdArray",
                    "dataType": "float",
                    "axisNames": ["t"],
                    "shape": [0],
                    "values": [],
                }

            time_query = (
                query.distinct()
                .order_by(self.tc.desc())
                .limit(limit)
                .subquery()
            )
            time_subquery = aliased(self.table_model, time_query)
            time_alias = getattr(time_subquery, self.time_field)

            # Create select columns for each parameter
            select_columns = [
                time_alias,
                *[
                    case(
                        (
                            and_(
                                parameter == self.pic, location_id == self.lc
                            ),
                            self.rc,
                        ),
                        else_=None,
                    ).label(parameter)
                    for parameter in parameters
                ],
            ]

            # Construct the query
            results = (
                session.query(*select_columns)
                .select_from(time_subquery)
                .outerjoin(
                    self.table_model,
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
                for parameter, value in row.items():
                    coverage["ranges"][parameter]["values"].append(value)
                    coverage["ranges"][parameter]["shape"][0] += 1

        if len(t_values) > 1:
            coverage["domain"]["domainType"] += "Series"

        return coverage

    def _sqlalchemy_to_feature(self, item, results, crs_transform_out=None):
        # Add properties from item

        datetime_view = func.concat(func.min(self.tc), "/", func.max(self.tc))
        datetime_range = results.with_entities(datetime_view).scalar()
        parameters = results.distinct(self.pic).with_entities(self.pic).all()
        parameters = [_[0] for _ in parameters]

        feature = {
            "type": "Feature",
            "id": rgetattr(item, self.location_field),
            "properties": {
                "datetime": datetime_range,
                "parameter-name": parameters,
            },
        }

        # Convert geometry to GeoJSON style
        if hasattr(item, self.geom):
            wkb_geom = rgetattr(item, self.geom)
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

    def _get_parameters(self, parameters: list = [], aslist=False):
        """
        Generate parameters

        :param datastream: The datastream data to generate parameters for.
        :param label: The label for the parameter.

        :returns: A dictionary containing the parameter definition.
        """
        if not parameters:
            parameters = self.fields.keys()

        out_params = {}
        for param in parameters:
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
