from datetime import date
from typing import Any, Optional

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
            "collection",
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


class SourceModel(ConfigBaseModel):
    source: str
    attribution: Optional[str]
    collection: Optional[str]
    documentation_url: Optional[str]
    endpoint: Optional[str]
    licence: Optional[str]
    organisation: Optional[str]
    pipelines: str
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]


class EndpointModel(ConfigBaseModel):
    endpoint: str
    endpoint_url: str
    parameters: Optional[str]
    plugin: Optional[str]
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]


class ConfigPipelineModel(ConfigBaseModel):
    dataset: Optional[str]
    endpoint: Optional[str]
    resource: Optional[str]


class ColumnModel(ConfigPipelineModel):
    column: Optional[str]
    field: Optional[str]
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]


class CombineModel(ConfigPipelineModel):
    field: Optional[str]
    separator: Optional[str]
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]


class ConcatModel(ConfigPipelineModel):
    field: Optional[str]
    fields: Optional[str]
    separator: Optional[str]
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]


class ConvertModel(ConfigPipelineModel):
    pluguin: Optional[str]
    parameters: Optional[str]
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]


class DefaultModel(ConfigPipelineModel):
    field: Optional[str]
    default_field: Optional[str]
    entry_number: Optional[int]
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]


class DefaultValueModel(ConfigPipelineModel):
    field: Optional[str]
    entry_number: Optional[int]
    value: Optional[str]


class LookupModel(ConfigPipelineModel):
    organisation: Optional[str]
    entity: Optional[int]
    entry_number: Optional[int]
    prefix: Optional[str]
    reference: Optional[str]
    value: Optional[str]
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]


class PatchModel(ConfigPipelineModel):
    field: Optional[str]
    entry_number: Optional[int]
    prefix: Optional[str]
    pattern: Optional[str]
    value: Optional[str]
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]


class SkipModel(ConfigPipelineModel):
    pattern: Optional[str]
    entry_number: Optional[int]
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]


class TransformModel(ConfigPipelineModel):
    field: Optional[str]
    replacement_field: Optional[str]
    entry_number: Optional[int]
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]


class FilterModel(ConfigPipelineModel):
    field: Optional[str]
    pattern: Optional[str]
    entry_number: Optional[int]
    entry_date: Optional[date]
    start_date: Optional[date]
    end_date: Optional[date]
