# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import functools
from typing import Any

from sqlalchemy import MetaData
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.sql import Select

from pygeoapi.provider.base import ProviderConnectionError

import logging

LOGGER = logging.getLogger(__name__)


def recursive_getattr(obj: Any, attr: str) -> Any:
    """
    Recursively traverse an object's attributes single dot
    notation and return the final node.
    """
    for part in attr.split("."):
        obj = getattr(obj, part)
    return obj


def get_column_from_qualified_name(model: Any, path: str) -> Any:
    """
    Resolves a dot-qualified SQLAlchemy attribute path to the actual column.
    Supports relationships and backrefs.
    """
    # Check if there are more tables to hop
    parts = path.split(".", 1)

    try:
        attr = getattr(model, parts[0])
    except AttributeError:
        return model

    if len(parts) == 1:
        return attr  # Final attribute — expected to be a column

    # Follow the relationship
    related_model = attr.mapper.class_
    return get_column_from_qualified_name(related_model, parts[1])


@functools.cache
def get_base_schema(tables: tuple[str], schema: str, engine):
    metadata = MetaData()

    try:
        # Reflect available tables first
        metadata.reflect(
            bind=engine,
            schema=schema,
            only=tables,
            views=True,
        )
    except OperationalError:
        raise ProviderConnectionError(
            f"Could not connect to {repr(engine.url)} (password hidden)."
        )

    _Base = automap_base(metadata=metadata)
    _Base.prepare(
        name_for_scalar_relationship=_name_for_scalar_relationship,
    )
    return _Base


def _name_for_scalar_relationship(base, local_cls, referred_cls, constraint):
    """Function used when automapping classes and relationships from
    database schema and fixes potential naming conflicts.
    """
    name = referred_cls.__name__.lower()
    local_table = local_cls.__table__
    if name in local_table.columns:
        newname = name + "_"
        LOGGER.debug(
            f"Already detected column name {name!r} in table "
            f"{local_table!r}. Using {newname!r} for relationship name."
        )
        return newname
    return name


def with_joins(self, joins, **kw):
    query = self
    for j in joins:
        query = query.join(*j, **kw)
    return query


Select.with_joins = with_joins
