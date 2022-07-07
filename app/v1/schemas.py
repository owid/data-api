import datetime as dt
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Extra


class VariableDataResponse(BaseModel):
    year: List[int]
    entity_name: List[str]
    entity_id: List[int]
    entity_code: List[str]
    value: List[Any]

    class Config:
        extra = Extra.forbid


class VariableDisplay(BaseModel):
    name: Optional[str]
    unit: Optional[str]
    shortUnit: Optional[str]
    includeInTable: Optional[bool]
    conversionFactor: Optional[float]

    class Config:
        extra = Extra.forbid


class VariableSource(BaseModel):
    id: int
    name: str
    dataPublishedBy: str
    dataPublisherSource: str
    link: str
    retrievedDate: str
    additionalInfo: str

    class Config:
        extra = Extra.forbid


class DimensionProperties(BaseModel):
    id: int
    name: Optional[str] = None
    code: Optional[str] = None

    class Config:
        extra = Extra.forbid


class Dimension(BaseModel):
    type: str
    values: List[DimensionProperties]

    class Config:
        extra = Extra.forbid


class VariableMetadataResponse(BaseModel):
    name: str
    unit: str
    shortUnit: Optional[str]
    code: Optional[str]
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
    originalMetadata: Optional[str]
    grapherConfig: Optional[str]
    # MAYBE CHANGE - this should be turned into an array
    source: VariableSource
    type: str
    dimensions: Dict[str, Dimension]

    class Config:
        extra = Extra.forbid
