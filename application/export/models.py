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
    documentation_url: Optional[str]
    endpoint: Optional[str]
    licence: Optional[str]
    organisation: Optional[str]


class EndpointModel(ConfigDateModel):
    endpoint: str
    endpoint_url: str
    parameters: Optional[str]
    plugin: Optional[str]


class ConfigPipelineModel(ConfigBaseModel):
    dataset: Optional[str]
    endpoint: Optional[str]
    resource: Optional[str]


class ColumnModel(ConfigPipelineModel):
    column: Optional[str]
    field: Optional[str]


class CombineModel(ConfigPipelineModel):
    field: Optional[str]
    separator: Optional[str]


class ConcatModel(ConfigPipelineModel):
    field: Optional[str]
    fields: Optional[str]
    separator: Optional[str]


class ConvertModel(ConfigPipelineModel):
    pluguin: Optional[str]
    parameters: Optional[str]


class DefaultModel(ConfigPipelineModel):
    field: Optional[str]
    default_field: Optional[str]
    entry_number: Optional[int]


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


class PatchModel(ConfigPipelineModel):
    field: Optional[str]
    entry_number: Optional[int]
    prefix: Optional[str]
    pattern: Optional[str]
    value: Optional[str]


class SkipModel(ConfigPipelineModel):
    pattern: Optional[str]
    entry_number: Optional[int]


class TransformModel(ConfigPipelineModel):
    field: Optional[str]
    replacement_field: Optional[str]
    entry_number: Optional[int]


class FilterModel(ConfigPipelineModel):
    field: Optional[str]
    pattern: Optional[str]
    entry_number: Optional[int]


class PipelineModel(ConfigBaseModel):
    pipeline: str
    column: Optional[List[ColumnModel]]
    combine: Optional[List[CombineModel]]
    concat: Optional[List[ConcatModel]]
    convert: Optional[List[ConvertModel]]
    default: Optional[List[DefaultModel]]
    default_value: Optional[List[DefaultValueModel]]
    lookup: Optional[List[LookupModel]]
    patch: Optional[List[PatchModel]]
    skip: Optional[List[SkipModel]]
    transform: Optional[List[TransformModel]]
    filter: Optional[List[FilterModel]]


class CollectionModel(ConfigBaseModel):
    collection: str
    name: str
    sources: Optional[List[SourceModel]]
    endpoints: Optional[List[EndpointModel]]
    pipeline: PipelineModel
