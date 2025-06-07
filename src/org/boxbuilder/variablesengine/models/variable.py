from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class Variable(BaseModel):
    id: str
    name: str
    entity_id: str
    is_input: bool = True
    is_persisted: bool = False
    function_name: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    description: Optional[str] = None

    @property
    def input_variables(self) -> list[str]:
        return self.metadata.get('input_variables', [])

    @property
    def foreign_key_entity(self) -> Optional[str]:
        """Returns the entity name this variable references, if it's a foreign key."""
        foreign_key = self.metadata.get('foreign_key', {})
        return foreign_key.get('entity')
