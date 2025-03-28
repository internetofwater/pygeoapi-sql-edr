# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

from sqlalchemy.orm import Session, InstrumentedAttribute
import datetime
import pytest

from pygeoapi_sql_edr.edr import EDRProvider
from pygeoapi_sql_edr.lib import get_column_from_qualified_name as gqname
from pygeoapi_sql_edr.lib import recursive_getattr as rgetattr


@pytest.fixture()
def config():
    return {
        "name": "PostgreSQL",
        "type": "feature",
        "data": {
            "host": "localhost",
            "dbname": "edr",
            "user": "postgres",
            "password": "changeMe",
            "search_path": ["capture"],
        },
        "id_field": "id",
        "table": "waterservices_daily",
        "geom_field": "geometry",
        "time_field": "time",
        "edr_fields": {
            "location_field": "monitoring_location_id",
            "result_field": "value",
            "parameter_id": "parameter_code",
            "parameter_name": "waterservices_timeseries_metadata.parameter_name",  # noqa
            "parameter_unit": "unit_of_measure",
        },
        "external_tables": {
            "waterservices_timeseries_metadata": {
                "foreign": "parameter_code",
                "remote": "parameter_code",
            }
        },
    }


def test_external_table_relationships(config):
    p = EDRProvider(config)

    assert p.table_model in p.table_models
    assert len(p.table_models) == 2

    for table in p.external_tables:
        assert hasattr(p.table_model, table)


def test_can_query_single_edr_cols(config):
    p = EDRProvider(config)
    edr_attrs = [p.tc, p.pic, p.pnc, p.puc, p.lc, p.rc]
    assert all([isinstance(f, InstrumentedAttribute) for f in edr_attrs])
    assert gqname(p.table_model, p.parameter_id) == p.pic

    edr_names = [
        p.time_field,
        p.parameter_id,
        p.parameter_name,
        p.parameter_unit,
        p.location_field,
        p.result_field,
    ]
    edr_vals = [
        datetime.date(1925, 4, 10),
        "00060",
        "Discharge",
        "ft^3/s",
        "USGS-11281500",
        129.0,
    ]
    with Session(p._engine) as session:
        result = session.query(p.table_model).first()
        for edr_name, edr_val in zip(edr_names, edr_vals):
            assert rgetattr(result, edr_name) == edr_val

    with Session(p._engine) as session:
        query = session.query(p.table_model)
        for j in p.joins:
            query = query.join(*j)

        for edr_attr, edr_val in zip(edr_attrs, edr_vals):
            result = query.with_entities(edr_attr).limit(1).scalar()
            assert result == edr_val


def test_fields(config):
    """Testing query for a valid JSON object with geometry"""
    p = EDRProvider(config)

    assert len(p.fields) == 7
    for k, v in p.fields.items():
        assert len(k) == 5
        assert [k_ in ["title", "type", "x-ogc-unit"] for k_ in v]

    selected_mappings = {
        "00010": {
            "type": "number",
            "title": "Temperature, water",
            "x-ogc-unit": "degC",
        },
        "00060": {
            "type": "number",
            "title": "Discharge",
            "x-ogc-unit": "ft^3/s",
        },
        "00065": {
            "type": "number",
            "title": "Gage height",
            "x-ogc-unit": "ft",
        },
    }
    for k, v in selected_mappings.items():
        assert p.fields[k] == v


def test_locations(config):
    p = EDRProvider(config)

    locations = p.locations()

    assert locations["type"] == "FeatureCollection"
    assert len(locations["features"]) == 23

    feature = locations["features"][0]
    assert feature["id"] == "USGS-01465798"
    assert feature["properties"]["datetime"] == "2024-11-17/2024-12-08"
    assert feature["properties"]["parameter-name"] == ["00060"]

    parameters = [p["id"] for p in locations["parameters"]]
    for f in locations["features"]:
        for param in f["properties"]["parameter-name"]:
            assert param in parameters


def test_locations_limit(config):
    p = EDRProvider(config)

    locations = p.locations(limit=1)
    assert locations["type"] == "FeatureCollection"
    assert len(locations["features"]) == 1

    locations = p.locations(limit=500)
    assert locations["type"] == "FeatureCollection"
    assert len(locations["features"]) == 23

    locations = p.locations(limit=5)
    assert locations["type"] == "FeatureCollection"
    assert len(locations["features"]) == 5

    parameters = [p["id"] for p in locations["parameters"]]
    for f in locations["features"]:
        for param in f["properties"]["parameter-name"]:
            assert param in parameters


def test_locations_bbox(config):
    p = EDRProvider(config)

    locations = p.locations(bbox=[-109, 31, -103, 37])
    assert len(locations["features"]) == 3


def test_locations_select_param(config):
    p = EDRProvider(config)

    locations = p.locations()
    assert len(locations["parameters"]) == 7

    locations = p.locations(select_properties=["00010"])
    assert len(locations["features"]) == 4
    assert len(locations["parameters"]) == 1

    locations = p.locations(select_properties=["00060"])
    assert len(locations["features"]) == 9
    assert len(locations["parameters"]) == 1

    locations = p.locations(select_properties=["00010", "00060"])
    assert len(locations["features"]) == 13
    assert len(locations["parameters"]) == 2


def test_get_location(config):
    p = EDRProvider(config)

    location = p.locations(location_id="USGS-01465798")
    assert [k in location for k in ["type", "domain", "parameters", "ranges"]]

    assert location["type"] == "Coverage"

    domain = location["domain"]
    assert domain["type"] == "Domain"
    assert domain["domainType"] == "PointSeries"

    assert domain["axes"]["x"]["values"] == [-74.98516031202179]
    assert domain["axes"]["y"]["values"] == [40.05695572943445]
    assert domain["axes"]["t"]["values"] == [
        datetime.date(2024, 12, 8),
        datetime.date(2024, 12, 5),
        datetime.date(2024, 12, 2),
        datetime.date(2024, 11, 20),
        datetime.date(2024, 11, 17),
    ]

    t_len = len(domain["axes"]["t"]["values"])
    assert t_len == 5
    assert t_len == len(set(domain["axes"]["t"]["values"]))

    assert [k in location for k in ["type", "domain", "parameters", "ranges"]]

    for param in location["parameters"]:
        assert param in location["ranges"]

    for range in location["ranges"].values():
        assert range["axisNames"][0] in domain["axes"]
        assert range["shape"][0] == t_len
        assert len(range["values"]) == t_len
        assert range["values"] == [5.08, 5.22, 4.5, 6.94, 8.39]
