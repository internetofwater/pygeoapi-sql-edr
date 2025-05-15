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
    # Split the fully qualified name into next relationship
    parts = qualified_name.split(".", 1)

    # Check if there are more tables to hop
    if len(parts) >= 2:
        nt, ft = parts
        hop = getattr(model, nt)
        return get_column_from_qualified_name(hop.mapper.class_, ft)

    else:
        return recursive_getattr(model, qualified_name)
