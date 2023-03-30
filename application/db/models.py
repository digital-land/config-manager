import enum

from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import declarative_mixin, declared_attr

from application.extensions import db


class PublicationStatus(enum.Enum):
    DRAFT = "DRAFT"
    PUBLISHED = "PUBLISHED"


@declarative_mixin
class VersionedMixin:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    version_id = db.Column(db.Integer, nullable=False, default=0)

    __mapper_args__ = {"version_id_col": version_id}


class DateModel(db.Model):
    __abstract__ = True

    entry_date = db.Column(db.Date)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)


source_dataset = db.Table(
    "source_dataset",
    db.Column("dataset_id", db.Text, db.ForeignKey("dataset.dataset")),
    db.Column("source_id", db.Text, db.ForeignKey("source.source")),
)

# read only models - i.e. read only copy of some specification tables

dataset_field = db.Table(
    "dataset_field",
    db.Column(
        "dataset_id", db.Text, db.ForeignKey("dataset.dataset"), primary_key=True
    ),
    db.Column("field_id", db.Text, db.ForeignKey("field.field"), primary_key=True),
    db.Column("hint", db.Text),
    db.Column("guidance", db.Text),
    db.Column("entry_date", db.TIMESTAMP),
    db.Column("start_date", db.Date),
    db.Column("end_date", db.Date),
)


class Collection(DateModel):
    collection = db.Column(db.Text, primary_key=True)
    name = db.Column(db.Text)
    pipeline = db.relationship("Pipeline", uselist=False, back_populates="collection")
    datasets = db.relationship("Dataset", back_populates="collection")
    sources = db.relationship("Source", back_populates="collection")

    publication_status = db.Column(
        db.Enum(PublicationStatus, name="publication_status"),
        default=PublicationStatus.DRAFT,
    )

    @property
    def endpoints(self):
        endpoints = set([])
        for s in self.sources:
            endpoints.add(s.endpoint)
        return endpoints


class Organisation(DateModel):
    organisation = db.Column(db.Text, primary_key=True)
    addressbase_custodian = db.Column(db.Text)
    billing_authority = db.Column(db.Text)
    census_area = db.Column(db.Text)
    combined_authority = db.Column(db.Text)
    company = db.Column(db.Text)
    entity = db.Column(db.BigInteger)
    esd_inventory = db.Column(db.Text)
    local_authority_type = db.Column(db.Text)
    local_resilience_forum = db.Column(db.Text)
    name = db.Column(db.Text)
    official_name = db.Column(db.Text)
    opendatacommunities_uri = db.Column(db.Text)
    parliament_thesaurus = db.Column(db.Text)
    prefix = db.Column(db.Text)
    reference = db.Column(db.Text)
    region = db.Column(db.Text)
    shielding_hub = db.Column(db.Text)
    statistical_geography = db.Column(db.Text)
    twitter = db.Column(db.Text)
    website = db.Column(db.Text)
    wikidata = db.Column(db.Text)
    wikipedia = db.Column(db.Text)
    sources = db.relationship(
        "Source", backref="organisation", lazy=True, order_by="Source.entry_date"
    )


class Dataset(DateModel):
    dataset = db.Column(db.Text, primary_key=True)
    attribution_id = db.Column(db.Text, db.ForeignKey("attribution.attribution"))
    collection_id = db.Column(db.Text, db.ForeignKey("collection.collection"))
    collection = db.relationship("Collection", back_populates="datasets")
    description = db.Column(db.Text)
    key_field = db.Column(db.Text)
    entity_minimum = db.Column(db.BigInteger)
    entity_maximum = db.Column(db.BigInteger)
    licence_id = db.Column(db.Text, db.ForeignKey("licence.licence"))
    name = db.Column(db.Text)
    paint_options = db.Column(JSON)
    plural = db.Column(db.Text)
    prefix = db.Column(db.Text)
    text = db.Column(db.Text)
    typology_id = db.Column(db.Text, db.ForeignKey("typology.typology"))
    typology = db.relationship("Typology")
    wikidata = db.Column(db.Text)
    wikipedia = db.Column(db.Text)
    fields = db.relationship("Field", secondary=dataset_field, lazy="subquery")

    sources = db.relationship(
        "Source", secondary=source_dataset, lazy="subquery", back_populates="datasets"
    )


class Typology(DateModel):
    typology = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text)
    description = db.Column(db.Text)
    text = db.Column(db.Text)
    plural = db.Column(db.Text)
    wikidata = db.Column(db.Text)
    wikipedia = db.Column(db.Text)
    fields = db.relationship("Field", backref="typology", lazy=True)


class Attribution(DateModel):
    attribution = db.Column(db.Text, primary_key=True)
    text = db.Column(db.Text)


class Licence(DateModel):
    licence = db.Column(db.Text, primary_key=True)
    text = db.Column(db.Text)


class Field(DateModel):
    field = db.Column(db.Text, primary_key=True, nullable=False)
    cardinality = db.Column(db.Text)
    description = db.Column(db.Text)
    guidance = db.Column(db.Text)
    hint = db.Column(db.Text)
    name = db.Column(db.Text)
    parent_field = db.Column(db.Text)
    replacement_field = db.Column(db.Text)
    text = db.Column(db.Text)
    uri_template = db.Column(db.Text)
    wikidata_property = db.Column(db.Text)
    datatype_id = db.Column(db.Text, db.ForeignKey("datatype.datatype"), nullable=True)
    typology_id = db.Column(db.Text, db.ForeignKey("typology.typology"), nullable=True)


class Datatype(DateModel):
    datatype = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text)
    text = db.Column(db.Text)
    fields = db.relationship("Field", backref="datatype", lazy=True)


# end read only models


class Pipeline(db.Model):
    pipeline = db.Column(db.Text, primary_key=True)
    name = db.Column(db.Text)

    collection_id = db.Column(db.Text, db.ForeignKey("collection.collection"))
    collection = db.relationship("Collection", back_populates="pipeline")

    column = db.relationship("Column", backref="pipeline")
    combine = db.relationship("Combine", backref="pipeline")
    concat = db.relationship("Concat", backref="pipeline")
    convert = db.relationship("Convert", backref="pipeline")
    default = db.relationship("Default", backref="pipeline")
    default_value = db.relationship("DefaultValue", backref="pipeline")
    filter = db.relationship("Filter", backref="pipeline")
    lookup = db.relationship("Lookup", backref="pipeline")
    patch = db.relationship("Patch", backref="pipeline")
    skip = db.relationship("Skip", backref="pipeline")
    transform = db.relationship("Transform", backref="pipeline")

    publication_status = db.Column(
        db.Enum(PublicationStatus, name="publication_status"),
        default=PublicationStatus.DRAFT,
    )

    # No lookups handled here yet - need a better way to process
    # them as there are so many
    def get_pipeline_rules(self):
        return {
            "column": self.column,
            "combine": self.combine,
            "concat": self.concat,
            "convert": self.convert,
            "default": self.default,
            "default_value": self.default_value,
            "filter": self.filter,
            "patch": self.patch,
            "skip": self.skip,
            "transform": self.transform,
        }


class Source(DateModel, VersionedMixin):
    source = db.Column(db.Text, primary_key=True)
    attribution_id = db.Column(db.Text, db.ForeignKey("attribution.attribution"))
    documentation_url = db.Column(db.Text)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=False)
    licence_id = db.Column(db.Text, db.ForeignKey("licence.licence"))
    organisation_id = db.Column(db.Text, db.ForeignKey("organisation.organisation"))
    collection_id = db.Column(db.Text, db.ForeignKey("collection.collection"))
    collection = db.relationship("Collection", back_populates="sources")

    attribution = db.relationship("Attribution")
    licence = db.relationship("Licence")

    datasets = db.relationship(
        "Dataset", secondary=source_dataset, lazy="subquery", back_populates="sources"
    )

    @property
    def pipelines(self):
        datasets = [d.dataset for d in self.datasets]
        if not datasets:
            return None
        if len(datasets) == 1:
            return datasets[0]
        else:
            return ";".join(datasets)

    def update(self, data):
        for key, val in data.items():
            if hasattr(self, key) and key not in [
                "datasets",
                "organisation",
                "collection",
            ]:
                if val == "":
                    val = None
                if key in ["attribution", "licence"]:
                    setattr(self, f"{key}_id", val)
                else:
                    setattr(self, key, val)
        self.collection.publication_status = PublicationStatus.DRAFT.name


class Endpoint(DateModel, VersionedMixin):
    endpoint = db.Column(db.Text, primary_key=True)
    endpoint_url = db.Column(db.Text)
    parameters = db.Column(db.Text)
    plugin = db.Column(db.Text)

    sources = db.relationship(
        "Source", backref="endpoint", lazy=True, order_by="Source.entry_date"
    )

    def to_csv_dict(self):
        return {
            "endpoint": self.endpoint,
            "endpoint-url": self.endpoint_url,
            "parameters": self.parameters,
            "plugin": self.plugin,
            "entry-date": self.entry_date,
            "start-date": self.start_date,
            "end-date": self.end_date,
        }


class Column(DateModel, VersionedMixin):
    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    column = db.Column(db.Text)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"), nullable=True)

    dataset = db.relationship("Dataset")
    endpoint = db.relationship("Endpoint")
    field = db.relationship("Field")


class Combine(DateModel, VersionedMixin):
    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"), nullable=True)
    separator = db.Column(db.Text)

    dataset = db.relationship("Dataset")
    endpoint = db.relationship("Endpoint")
    field = db.relationship("Field")


class Concat(DateModel, VersionedMixin):
    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"), nullable=True)
    fields = db.Column(db.Text)
    separator = db.Column(db.Text)

    dataset = db.relationship("Dataset")
    endpoint = db.relationship("Endpoint")
    field = db.relationship("Field")


class Convert(DateModel, VersionedMixin):
    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    plugin = db.Column(db.Text)
    parameters = db.Column(db.Text)

    dataset = db.relationship("Dataset")
    endpoint = db.relationship("Endpoint")


class Default(DateModel, VersionedMixin):
    __tablename__ = "_default"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"), nullable=True)
    default_field = db.Column(db.Text)
    entry_number = db.Column(db.BigInteger)

    dataset = db.relationship("Dataset")
    endpoint = db.relationship("Endpoint")
    field = db.relationship("Field")


class DefaultValue(DateModel, VersionedMixin):
    __tablename__ = "default_value"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"))
    entry_number = db.Column(db.BigInteger)
    value = db.Column(db.Text)

    dataset = db.relationship("Dataset")
    endpoint = db.relationship("Endpoint")
    field = db.relationship("Field")


class Lookup(DateModel, VersionedMixin):
    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    organisation_id = db.Column(
        db.Text, db.ForeignKey("organisation.organisation"), nullable=True
    )
    resource = db.Column(db.Text)
    entity = db.Column(db.BigInteger)
    entry_number = db.Column(db.BigInteger)
    prefix = db.Column(db.Text)
    reference = db.Column(db.Text)
    value = db.Column(db.Text)

    endpoint = db.relationship("Endpoint")
    organisation = db.relationship("Organisation")


class Patch(DateModel, VersionedMixin):
    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"), nullable=True)
    entry_number = db.Column(db.BigInteger)
    pattern = db.Column(db.Text)
    value = db.Column(db.Text)

    dataset = db.relationship("Dataset")
    endpoint = db.relationship("Endpoint")
    field = db.relationship("Field")


class Skip(DateModel, VersionedMixin):
    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    pattern = db.Column(db.Text)
    entry_number = db.Column(db.BigInteger)

    dataset = db.relationship("Dataset")
    endpoint = db.relationship("Endpoint")


class Transform(DateModel, VersionedMixin):
    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"), nullable=True)
    replacement_field = db.Column(db.Text)
    entry_number = db.Column(db.BigInteger)

    dataset = db.relationship("Dataset")
    endpoint = db.relationship("Endpoint")
    field = db.relationship("Field")


class Filter(DateModel, VersionedMixin):
    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"), nullable=True)
    pattern = db.Column(db.Text)
    entry_number = db.Column(db.BigInteger)

    dataset = db.relationship("Dataset")
    endpoint = db.relationship("Endpoint")
    field = db.relationship("Field")
