from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from org.boxbuilder.database.postgres.models.data_types import DataTypes


class TableModel(BaseModel):
    """
    Represents a table or view within a specific database schema, including the table's
    name, columns, and data types, as well as other metadata such as primary keys and
    whether the object is a view or a table.

    Attributes:
    database (str): The name of the database where the table or view resides.
    schema (str): The schema within the database where the table or view is located.
    table (str): The name of the table or view.
    primary_keys (Optional[List[str]]): A list of column names that represent the primary keys of the table.
                                        Defaults to None if no primary keys are defined.
    column_name_to_data_type_map (Dict[str, DataTypes]): A mapping of column names to their respective data types.
                                                         This is a dictionary where keys are column names and values
                                                         are instances of the `DataTypes` enum.

    Methods:
    get_fqn() -> str:
        Returns the fully qualified name (FQN) of the table or view in the format:
        `"<schema>"."<table>`, where each part is quoted.
    """

    database: str
    schema: str
    table: str
    primary_keys: Optional[List[str]] = None
    column_name_to_data_type_map: Dict[str, DataTypes] = Field(default_factory=dict)

    def get_fqn(self) -> str:
        """
        Generates the fully qualified name (FQN) of the table or view, formatted as:
        `"<schema>"."<table>"`.

        Returns:
        str: The fully qualified name (FQN) of the table or view, with each component quoted.

        Example:
        >>> table = TableModel(database="my_db", schema="public", table="users")
        >>> table.get_fqn()
        '"public"."users"'
        """
        return ".".join([f'"{part}"' for part in [self.schema, self.table]])
