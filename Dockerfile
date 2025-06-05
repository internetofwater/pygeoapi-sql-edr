# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

FROM geopython/pygeoapi:latest

COPY *.toml *.yml /pygeoapi-sql-edr/
COPY src/ /pygeoapi-sql-edr/src/

RUN pip install -e /pygeoapi-sql-edr 
RUN apt-get update \
    && apt-get install -y git \
    && git clone https://github.com/C-Loftus/pygeoapi.git /tmp \
    && rm -rf /pygeoapi \
    && mv -f /tmp/pygeoapi /pygeoapi

ENV PYGEOAPI_CONFIG=/pygeoapi-sql-edr/pygeoapi.config.yml
ENV PYGEOAPI_OPENAPI=/pygeoapi-sql-edr/pygeoapi.openapi.yml

COPY docker/entrypoint.sh /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]
