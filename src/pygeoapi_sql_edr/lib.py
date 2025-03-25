# Copyright 2025 Lincoln Institute of Land Policy
# SPDX-License-Identifier: MIT

import logging

LOGGER = logging.getLogger(__name__)


def recursive_getattr(obj, attr):
    """Recursively traverse an object's attributes using dot notation and return the final node."""
    try:
        for part in attr.split("."):
            obj = getattr(obj, part)  # Use getattr, not recursive_getattr
        return obj
    except AttributeError:
        return


def get_column_from_qualified_name(model, qualified_name):
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
