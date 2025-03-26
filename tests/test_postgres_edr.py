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


def test_can_single_edr_cols(config):
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

    assert len(p.fields) == 8
    for k, v in p.fields.items():
        assert len(k) == 5
        assert [k_ in ["title", "typex-ogc-unit"] for k_ in v]

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
