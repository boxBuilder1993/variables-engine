from typing import List

from pydantic import BaseModel, Field

from org.boxbuilder.variablesengine.models.entity import Entity
from org.boxbuilder.variablesengine.models.variable import Variable


class Project(BaseModel):
    id: str
    name: str
    entities: List[Entity] = Field(default_factory=list)
    variables: List[Variable] = Field(default_factory=list)
