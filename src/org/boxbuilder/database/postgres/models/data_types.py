import json
from enum import Enum
from datetime import datetime, date, time
from decimal import Decimal
from typing import Any, Optional, Union

from pydantic import BaseModel

_TIME_DATA_TYPE_FORMATS = {
    "TIME": "%H:%M:%S",
    "DATE": "%Y-%m-%d",
    "TIMESTAMP": "%Y-%m-%d %H:%M:%S",
    "TIMESTAMP WITH TIME ZONE": "%Y-%m-%d %H:%M:%S%z",
}


class DataTypes(Enum):
    """
    Enum class representing different data types and their associated Pandas data types.

    Attributes:
    TIME: A time representation as a string.
    DATE: A date representation as a string.
    TIMESTAMP_WITH_TIMEZONE: A timestamp with timezone, represented as `datetime64[ns, UTC]`.
    TIMESTAMP: A timestamp representation, represented as `datetime64[ns]`.
    TEXT: A text representation, represented as a string.
    BOOLEAN: A boolean representation, represented as a boolean.
    NUMBER: A numeric representation, represented as a float.

    Methods:
    validate_and_convert(value: Any) -> Optional[Any]:
        Validates and converts the given value to the corresponding data type.
    """

    TIME = ("TIME", "string")
    DATE = ("DATE", "string")
    TIMESTAMP_WITH_TIMEZONE = ("TIMESTAMP WITH TIME ZONE", "datetime64[ns, UTC]")
    TIMESTAMP = ("TIMESTAMP", "datetime64[ns]")
    TEXT = ("TEXT", "string")
    BOOLEAN = ("BOOLEAN", "boolean")
    NUMBER = ("NUMERIC", "float64")
    JSON = ("JSONB", "str")

    def __init__(self, name: str, pandas_dtype: str):
        """
        Initialize a data type enumeration with its string name and corresponding Pandas data type.

        Parameters:
        name (str): The name of the data type.
        pandas_dtype (str): The corresponding Pandas data type representation.
        """
        self._name = name
        self.pandas_dtype = pandas_dtype

    def validate_and_convert(self, value: Any) -> Optional[Any]:
        """
        Validates and converts the given value to the corresponding data type.

        Parameters:
        value (Any): The value to be validated and converted.

        Returns:
        Optional[Any]: The value converted to the corresponding data type, or None if the value is None.

        Raises:
        ValueError: If the value cannot be converted to the expected data type.
        """
        if value is None:
            return None

        if self == DataTypes.TEXT:
            return str(value)
        if self == DataTypes.NUMBER:
            return Decimal(value)
        if self == DataTypes.BOOLEAN:
            if isinstance(value, str):
                if value.upper() == "TRUE":
                    return True
                elif value.upper() == "FALSE":
                    return False
                else:
                    raise RuntimeError(f"Unable to convert {value} into boolean.")
            return bool(value)
        if self == DataTypes.JSON:
            if isinstance(value, str):
                try:
                    json.loads(value)
                except Exception as e:
                    raise RuntimeError(f"Invalid JSON string found. Parsing error: {e}")
            elif isinstance(value, BaseModel):
                return value.model_dump_json()
            else:
                return json.dumps(value)
        if self in {
            DataTypes.TIMESTAMP,
            DataTypes.TIMESTAMP_WITH_TIMEZONE,
            DataTypes.DATE,
            DataTypes.TIME,
        }:
            return self._validate_and_convert_datetime(value)

        raise RuntimeError(f"Unsupported data type {self._name}")

    def _validate_and_convert_datetime(self, value: Any) -> Optional[Any]:
        format_str = _TIME_DATA_TYPE_FORMATS.get(self._name)
        if format_str is None:
            raise RuntimeError(
                f"Timestamp format not configured for data type: {self._name}"
            )

        if isinstance(value, (datetime, date, time)):
            return self._coerce_to_appropriate_datetime_type(value)

        if isinstance(value, str):
            try:
                parsed_value = datetime.strptime(value, format_str)
                return self._coerce_to_appropriate_datetime_type(parsed_value)
            except ValueError:
                raise ValueError(
                    f"Invalid format for {self._name}: {value}. Expected format: {format_str}"
                )

        raise ValueError(
            f"Invalid type for {self._name}: {type(value)}. Expected string, datetime, date, or time."
        )

    def _coerce_to_appropriate_datetime_type(
        self, parsed_value: Union[datetime, date, time]
    ) -> Union[datetime, date, time]:
        if self == DataTypes.DATE:
            if isinstance(parsed_value, datetime):
                return parsed_value.date()
            if isinstance(parsed_value, date):
                return parsed_value
            raise RuntimeError(
                f"Unable to convert {type(parsed_value)} into datetime.date format."
            )
        if self == DataTypes.TIME:
            if isinstance(parsed_value, datetime):
                return parsed_value.time()
            if isinstance(parsed_value, time):
                return parsed_value
            raise RuntimeError(
                f"Unable to convert {type(parsed_value)} into datetime.time format."
            )

        if self == DataTypes.TIMESTAMP_WITH_TIMEZONE and parsed_value.tzinfo is None:
            raise ValueError(
                f"TIMESTAMP WITH TIME ZONE requires a timezone: {parsed_value}"
            )
        if self == DataTypes.TIMESTAMP and parsed_value.tzinfo is not None:
            raise ValueError(f"TIMESTAMP should not have a timezone: {parsed_value}")

        return parsed_value
