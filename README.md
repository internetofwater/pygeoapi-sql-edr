# pygeoapi Environmental Data Retrieval

This repository contains SQL pygeoapi providers for OGC API - Environmental Data Retrieval.

## OGC API - EDR

The configuration for SQL OGC API - EDR follows that of pygeoapi for [OGC API - Features](https://docs.pygeoapi.io/en/latest/data-publishing/ogcapi-features.html#postgresql), with the addition of two sections `edr_fields` and `external_tables`.
For more detailed documentation on the creation of a pygeoapi configuration file, refer
to the [docs](https://docs.pygeoapi.io/en/latest/configuration.html).

The `edr_fields` section defines the columns of your SQL table and their corresponding field in OGC API - EDR.
The `external_tables` section allows foriegn table joins to allow `edr_fields` to refer to any table/column with a mapped relationship to the primary table.

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
    id_field: id # Result identifier field
    geom_field: geometry # Result identifier field
    time_field: time # Result time field
    location_field: monitoring_location_id # Result location identifier field
    result_field: value # Result value field
    parameter_id: parameter_code # Result parameter id field
    parameter_name: waterservices_timeseries_metadata.parameter_name # Result parameter name field
    parameter_unit: unit_of_measure # Result parameter unit field

  external_tables: # Additional table joins
    waterservices_timeseries_metadata: # JOIN waterservices_timeseries_metadata ON waterservices_daily.parameter_code=waterservices_timeseries_metadata.parameter_code
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
    airports: # JOIN airports ON landing_observations.location_id=airports.code
      foreign: location_id
      remote: code
    airports.airport_locations: # JOIN airport_locations ON airports.code=airport_locations.id
      foreign: code
      remote: id
    airport_parameters: # JOIN airport_parameters ON landing_observations.parameter_id=airport_parameters.id
      foreign: parameter_id
      remote: id

```
