import hashlib
from typing import Callable, Any, List, Optional
import pandas as pd


def build_hash_column(
    df: pd.DataFrame,
    hash_column_name: str = "hash",
    include_columns: Optional[List[str]] = None,
    value_to_str_converter: Callable[[Any], str] = lambda v: str(v),
):
    """
    Adds a new column to a DataFrame with hashed values based on specified columns.

    This function computes a SHA-256 hash for each row in the DataFrame by combining
    the values from the specified columns (or all columns if none are specified),
    and adds a new column with the resulting hash values.

    Parameters:
    df (pd.DataFrame): The DataFrame to which the new hash column will be added.
    hash_column_name (str): The name of the new column to store the hash values. Default is "hash".
    include_columns (Optional[List[str]]): A list of column names to include in the hash calculation.
                                            If None, all columns are included. Default is None.
    value_to_str_converter (Callable[[Any], str]): A function that converts column values to a string
                                                   for hash calculation. Default is `lambda v: str(v)`.

    Returns:
    None: The function modifies the input DataFrame in-place by adding the hash column.

    Example:
    >>> df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    >>> build_hash_column(df, hash_column_name="row_hash", include_columns=["col1"])
    >>> print(df)
       col1 col2                                            row_hash
    0     1    a  e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
    1     2    b  0a0f9a4deff7e0ecf32b8b89f0cd06352f804b1a1a7775c9f0c567e777b20b2a

    """
    df[hash_column_name] = df.apply(
        lambda row: _hash_row(
            row=row,
            include_columns=include_columns,
            value_to_str_converter=value_to_str_converter,
        ),
        axis=1,
    )


def _hash_row(
    row: pd.Series,
    value_to_str_converter: Callable[[Any], str],
    include_columns: Optional[List[str]],
) -> str:
    row_str = "|".join(
        [
            value_to_str_converter(v)
            for k, v in row.items()
            if (include_columns is not None and k in include_columns)
            or include_columns is None
        ]
    )
    return hashlib.sha256(row_str.encode()).hexdigest()
