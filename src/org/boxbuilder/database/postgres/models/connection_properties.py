import os
from pydantic import BaseModel


class ConnectionProperties(BaseModel):
    """
    A class to represent the properties required for a PostgreSQL connection.

    Attributes:
    url (str): The database host URL.
    user_name (str): The username for the database connection.
    password (str): The password for the database connection.

    Methods:
    build_postgres_connection_url() -> str:
        Constructs the full PostgreSQL connection URL based on the provided attributes.
    """

    url: str
    user_name: str
    password: str

    def build_postgres_connection_url(self, database_name: str) -> str:
        """
        Constructs the PostgreSQL connection URL.

        The URL is generated in the format:
        "postgresql://<user_name>:<password>@<url>/<database_name>" if a `database_name` is provided,
        or "postgresql://<user_name>:<password>@<url>" if `database_name` is not provided.

        Returns:
        str: The full PostgreSQL connection URL.

        Example:
        >>> connection = ConnectionProperties(url="localhost", user_name="admin", password="secret")
        >>> connection.build_postgres_connection_url()
        'postgresql://admin:secret@localhost'
        >>> connection.database_name = "my_database"
        >>> connection.build_postgres_connection_url()
        'postgresql://admin:secret@localhost/my_database'
        """
        base_url = f"postgresql://{self.user_name}:{self.password}@{self.url}"
        return f"{base_url}/{database_name}"


def build_from_env_variables() -> ConnectionProperties:
    """
    Constructs a `ConnectionProperties` object using environment variables.

    The following environment variables are expected to be set:
    - POSTGRES_DB_URL: The database host URL.
    - POSTGRES_DB_USER_NAME: The username for the database connection.
    - POSTGRES_DB_PASSWORD: The password for the database connection.

    If any of the first three environment variables (`POSTGRES_DB_URL`, `POSTGRES_DB_USER_NAME`, `POSTGRES_DB_PASSWORD`) are missing,
    this function raises a `ValueError`.

    Returns:
    ConnectionProperties: A `ConnectionProperties` object containing the values from the environment variables.

    Raises:
    ValueError: If any of the first three required environment variables are missing.

    Example:
    >>> os.environ["POSTGRES_DB_URL"] = "localhost"
    >>> os.environ["POSTGRES_DB_USER_NAME"] = "admin"
    >>> os.environ["POSTGRES_DB_PASSWORD"] = "secret"
    >>> conn_props = build_from_env_variables()
    >>> print(conn_props.url)
    'localhost'
    >>> print(conn_props.user_name)
    'admin'
    >>> print(conn_props.password)
    'secret'
    """
    url = os.environ.get("POSTGRES_DB_URL")
    user_name = os.environ.get("POSTGRES_DB_USER_NAME")
    password = os.environ.get("POSTGRES_DB_PASSWORD")

    # Check for missing environment variables
    if url is None or user_name is None or password is None:
        raise ValueError(
            "Missing required environment variables: POSTGRES_DB_URL, POSTGRES_DB_USER_NAME, or POSTGRES_DB_PASSWORD"
        )

    return ConnectionProperties(url=url, user_name=user_name, password=password)
