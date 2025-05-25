from typing import Optional

from pydantic import BaseModel


class Variable(BaseModel):
    name: str
    is_input: bool
    is_persisted: Optional[bool] = None
    function_name: Optional[str] = None
