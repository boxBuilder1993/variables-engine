from typing import Optional, Any


def coalesce(*values) -> Optional[Any]:
    """
    Returns the first non-None value from the given arguments.

    Parameters:
    *values (Any): A variable number of arguments to check for non-None values.

    Returns:
    Optional[Any]: The first non-None value, or None if all values are None.

    Example:
    >>> coalesce(None, None, 3, 4)
    3

    >>> coalesce(None, 'hello', 'world')
    'hello'

    >>> coalesce(None, None)
    None
    """
    for v in values:
        if v is not None:
            return v
    return None
