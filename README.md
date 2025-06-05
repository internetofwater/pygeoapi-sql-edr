# pygeoapi Environmental Data Retrieval

This repository contains SQL pygeoapi providers for OGC API - Environmental Data Retrieval.

## OGC API - EDR

### Postgres

The configuration for Postgres EDR is as follows:

```yaml
- type: edr
  name: pg_edr.PostgresEDRProvider
  data: # Same as PostgresSQLProvider
    host: ${POSTGRES_HOST}
    dbname: ${POSTGRES_DB}
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}
    search_path: [capture]
  table: waterservices_daily

  edr_fields: # Required EDR Fields
    id_field: id
    geom_field: geometry
    time_field: time
    location_field: monitoring_location_id
    result_field: value
    parameter_id: parameter_code
    parameter_name: waterservices_timeseries_metadata.parameter_name
    parameter_unit: unit_of_measure

  external_tables: # Additional table joins
    waterservices_timeseries_metadata:
      foreign: parameter_code
      remote: parameter_code
```

### MySQL

The configuration for MySQL EDR is as follows:

```yaml
- type: edr
  name: pg_edr.edr.MySQLEDRProvider
  data: # Same as MySQLProvider
    host: ${MYSQL_HOST}
    port: ${MYSQL_PORT}
    dbname: ${MYSQL_DATABASE}
    user: ${MYSQL_USER}
    password: ${MYSQL_PASSWORD}
    search_path: [${MYSQL_DATABASE}]
  table: landing_observations
  edr_fields: # Required EDR Fields
    id_field: id
    geom_field: airports.airport_locations.geometry_wkt
    time_field: time
    location_field: location_id
    result_field: value
    parameter_id: parameter_id
    parameter_name: airport_parameters.name
    parameter_unit: airport_parameters.units

  external_tables: # Additional table joins
    airports:
      foreign: location_id
      remote: code
    airports.airport_locations:
      foreign: code
      remote: id
    airport_parameters:
      foreign: parameter_id
      remote: id

```
