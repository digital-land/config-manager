from datetime import date
from typing import Any, List, Optional

from pydantic import BaseModel
from pydantic.utils import GetterDict


def _to_kebab_case(field_name: str) -> str:
    return field_name.replace("_", "-")


# this all feels a bit hacky - is there a better way to customise from_orm?
class RelatedObjectKeyGetter(GetterDict):
    def get(self, key: str, default: Any) -> Any:
        if key in {
            "dataset",
            "attribution",
            "organisation",
            "licence",
            "endpoint",
            "field",
        }:
            if self._obj.__tablename__ != key:
                obj = getattr(self._obj, key, None)
                if obj:
                    return getattr(obj, key, None)
        return super().get(key, default)


class ConfigBaseModel(BaseModel):
    class Config:
        alias_generator = _to_kebab_case
        allow_population_by_field_name = True
        orm_mode = True
        arbitrary_types_allowed = True
        getter_dict = RelatedObjectKeyGetter


class ConfigDateModel(ConfigBaseModel):
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]


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
    dataset: Optional[str]
    endpoint: Optional[str]
    resource: Optional[str]
    column: str
    field: str


class PipelineModel(ConfigBaseModel):
    dataset: str
    sources: Optional[List[SourceModel]]
    endpoints: Optional[List[EndpointModel]]
    columns: Optional[List[ColumnModel]]
