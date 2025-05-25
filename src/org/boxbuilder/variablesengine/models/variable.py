from typing import Optional, List

from pydantic import BaseModel, Field


class Variable(BaseModel):
    id: str
    name: str
    is_input: bool
    is_persisted: Optional[bool] = None
    function_name: Optional[str] = None
    entity_id: str
    input_variables: List[str] = Field(default_factory=list)
