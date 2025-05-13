# pygeoapi Environmental Data Retrieval

This repository contains SQL pygeoapi providers for OGC API - Environmental Data Retrieval.

## OGC API - EDR

### Postgres

The configuration for Postgres EDR is as follows:

```yaml
- type: edr
  name: pygeoapi_sql_edr.edr.PostgresEDRProvider
  data: # Same as PostgresSQLProvider
    host: ${POSTGRES_HOST}
    dbname: ${POSTGRES_DB}
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}
    search_path: [capture]
  table: waterservices_daily
  id_field: id
  geom_field: geometry
  time_field: time
  edr_fields: # Additional fields not used in Features
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
