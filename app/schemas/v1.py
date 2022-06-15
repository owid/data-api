import datetime as dt
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class VariableDataResponse(BaseModel):
    year: List[int]
    entity_name: List[str]
    entity_id: List[int]
    entity_code: List[str]
    value: List[Any]


class VariableDisplay(BaseModel):
    name: Optional[str]
    unit: Optional[str]
    shortUnit: Optional[str]
    includeInTable: Optional[bool]


class VariableSource(BaseModel):
    id: int
    name: str
    dataPublishedBy: str
    dataPublisherSource: str
    link: str
    retrievedDate: str
    additionalInfo: str


class DimensionProperties(BaseModel):
    id: int
    name: Optional[str] = None
    code: Optional[str] = None


class Dimension(BaseModel):
    type: str
    values: List[DimensionProperties]


class Dimensions(BaseModel):
    years: Dimension
    entities: Dimension


class VariableMetadataResponse(BaseModel):
    name: str
    unit: str
    description: str
    createdAt: dt.datetime
    updatedAt: dt.datetime
    coverage: str
    timespan: str
    datasetId: int
    columnOrder: int
    datasetName: str
    nonRedistributable: bool
    display: VariableDisplay
    # MAYBE CHANGE - this should be turned into an array
    source: VariableSource
    type: str
    dimensions: Dimensions
