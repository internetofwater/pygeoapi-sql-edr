# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

from typing import Any

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


def get_column_from_qualified_name(model: Any, qualified_name: str) -> Any:
    # Split the fully qualified name into table name and column name
    parts = qualified_name.split(".")

    # Check if the parts are of different table
    if len(parts) == 2:
        table_name, column_name = parts

        table = getattr(model, table_name)
        if table:
            column = getattr(table.mapper.class_, column_name)
            if column:
                return column

    else:
        return getattr(model, qualified_name)
