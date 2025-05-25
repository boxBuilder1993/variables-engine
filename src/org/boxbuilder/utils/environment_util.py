"""
Module for retrieving environment-specific configurations.

This module provides functions to fetch critical environment variables,
such as the content root directory and the runtime environment.

Raises:
    RuntimeError: If required environment variables are not set.

Functions:
    get_content_root() -> Path:
        Retrieves the content root directory from the environment variable.

    get_environment() -> Environments:
        Retrieves the runtime environment from the environment variable
        and returns it as an `Environments` enum.
"""

import os
from pathlib import Path

from org.boxbuilder.utils.models.environments import Environments

CONTENT_ROOT_ENV_KEY = "CONTENT_ROOT"
RUNTIME_ENVIRONMENT_ENV_KEY = "RUNTIME_ENVIRONMENT"


def get_content_root() -> Path:
    """
    Retrieves the content root directory from an environment variable.

    The function reads the value of the environment variable defined by `_CONTENT_ROOT_KEY`
    and returns it as a `Path` object.

    Returns:
        Path: The content root directory.

    Raises:
        RuntimeError: If the environment variable is not set.
    """
    cr = os.getenv(CONTENT_ROOT_ENV_KEY)
    if cr is None:
        raise RuntimeError(
            f"Expecting {CONTENT_ROOT_ENV_KEY} environment variable to be configured."
        )
    return Path(cr)


def get_environment() -> Environments:
    """
    Retrieves the runtime environment from an environment variable.

    The function reads the value of the environment variable defined by `_RUNTIME_ENVIRONMENT_KEY`
    and returns the corresponding `Environments` enum.

    Returns:
        Environments: The runtime environment as an `Environments` enum.

    Raises:
        KeyError: If the environment variable is not set or does not match an existing enum member.
    """
    er = os.getenv(RUNTIME_ENVIRONMENT_ENV_KEY)
    return Environments[er]
