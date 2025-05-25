from typing import List

from pydantic import BaseModel, Field

from org.boxbuilder.variablesengine.models.variable import Variable


class Project(BaseModel):
    name: str
    variables: List[Variable] = Field(default_factory=list)
