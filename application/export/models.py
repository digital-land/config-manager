from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field


def _for_csv_file(field_name: str) -> str:
    if field_name.endswith("_id"):
        field_name = field_name.replace("_id", "")
    return field_name.replace("_", "-")


def _format_date(d: date) -> str:
    return d.isoformat()


class ConfigBaseModel(BaseModel):
    class Config:
        allow_population_by_field_name = True
        orm_mode = True
        arbitrary_types_allowed = True

        json_encoders = {date: _format_date}


class ConfigDateModel(ConfigBaseModel):
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]

    class Config:
        alias_generator = _for_csv_file


class SourceModel(ConfigDateModel):
    source: str
    attribution: Optional[str]
    documentation_url: str
    endpoint: Optional[str]
    licence: Optional[str]
    organisation: Optional[str]


class EndpointModel(ConfigDateModel):
    endpoint: str
    endpoint_url: str
    parameters: Optional[str]
    plugin: Optional[str]


class ColumnModel(ConfigDateModel):
    dataset: Optional[str] = Field(alias="dataset_id")
    endpoint: Optional[str] = Field(alias="endpoint_id")
    resource: Optional[str]
    column: str
    field: str = Field(alias="field_id")


class PipelineModel(ConfigBaseModel):
    dataset: str = Field(alias="dataset_id")
    sources: Optional[List[SourceModel]]
    # endpoints: Optional[List[EndpointModel]]
    # columns: Optional[List[ColumnModel]]
