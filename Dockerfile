# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

FROM geopython/pygeoapi:latest

COPY . /pygeoapi-sql-edr

RUN pip install --no-deps pygeoapi-plugins
RUN pip install -e /pygeoapi-sql-edr

ENV PYGEOAPI_CONFIG=/pygeoapi-sql-edr/pygeoapi.config.yml
ENV PYGEOAPI_OPENAPI=/pygeoapi-sql-edr/pygeoapi.openapi.yml

ENTRYPOINT [ "/pygeoapi-sql-edr/docker/entrypoint.sh" ]
