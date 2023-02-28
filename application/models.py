from sqlalchemy.dialects.postgresql import JSON

from application.extensions import db


class DateModel(db.Model):
    __abstract__ = True

    entry_date = db.Column(db.Date)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)


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


# read only models
class Organisation(DateModel):
    __tablename__ = "organisation"

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
    __tablename__ = "dataset"

    dataset = db.Column(db.Text, primary_key=True)
    attribution_id = db.Column(db.Text, db.ForeignKey("attribution.attribution"))
    collection = db.Column(db.Text)
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
    pipeline = db.relationship("Pipeline", uselist=False, back_populates="dataset")


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
    __tablename__ = "attribution"

    attribution = db.Column(db.Text, primary_key=True)
    text = db.Column(db.Text)


class Licence(DateModel):
    __tablename__ = "licence"

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
    columns = db.relationship("Column", backref="field", lazy=True)


class Datatype(DateModel):
    datatype = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text)
    text = db.Column(db.Text)
    fields = db.relationship("Field", backref="datatype", lazy=True)


# end read only models


source_pipeline = db.Table(
    "source_pipeline",
    db.Column("pipeline_id", db.Text, db.ForeignKey("pipeline.pipeline")),
    db.Column("source_id", db.Text, db.ForeignKey("source.source")),
)


class Pipeline(db.Model):
    __tablename__ = "pipeline"

    pipeline = db.Column(db.Text, primary_key=True)
    name = db.Column(db.Text)
    dataset = db.relationship("Dataset")
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=False)

    sources = db.relationship(
        "Source",
        secondary=source_pipeline,
        lazy="subquery",
        backref=db.backref("pipelines", lazy=True),
    )

    columns = db.relationship("Column")
    combines = db.relationship("Combine")
    concats = db.relationship("Concat")
    converts = db.relationship("Convert")
    defaults = db.relationship("Default")
    default_values = db.relationship("DefaultValue")
    filters = db.relationship("Filter")
    lookups = db.relationship("Lookup")
    patches = db.relationship("Patch")
    skips = db.relationship("Skip")
    transforms = db.relationship("Transform")


class Source(DateModel):
    __tablename__ = "source"

    source = db.Column(db.Text, primary_key=True)
    attribution_id = db.Column(db.Text, db.ForeignKey("attribution.attribution"))
    documentation_url = db.Column(db.Text)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    licence_id = db.Column(db.Text, db.ForeignKey("licence.licence"))
    organisation_id = db.Column(db.Text, db.ForeignKey("organisation.organisation"))


class Endpoint(DateModel):
    __tablename__ = "endpoint"

    endpoint = db.Column(db.Text, primary_key=True)
    endpoint_url = db.Column(db.Text)
    parameters = db.Column(db.Text)
    plugin = db.Column(db.Text)

    sources = db.relationship(
        "Source", backref="endpoint", lazy=True, order_by="Source.entry_date"
    )


class Column(DateModel):
    __tablename__ = "column"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    column = db.Column(db.Text)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"))


class Combine(DateModel):
    __tablename__ = "combine"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"))
    separator = db.Column(db.Text)


class Concat(DateModel):
    __tablename__ = "concat"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"))
    fields = db.Column(db.Text)
    separator = db.Column(db.Text)


class Convert(DateModel):
    __tablename__ = "convert"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    plugin = db.Column(db.Text)


class Default(DateModel):
    __tablename__ = "_default"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field = db.Column(db.Text, db.ForeignKey("field.field"))
    default_field = db.Column(db.Text)


class DefaultValue(DateModel):
    __tablename__ = "default_value"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"))
    entry_number = db.Column(db.BigInteger)
    value = db.Column(db.Text)


class Lookup(DateModel):
    __tablename__ = "lookup"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    organisation_id = db.Column(
        db.Text, db.ForeignKey("organisation.organisation"), nullable=False
    )
    resource = db.Column(db.Text)
    entity = db.Column(db.BigInteger)
    entry_number = db.Column(db.BigInteger)
    prefix = db.Column(db.Text)
    reference = db.Column(db.Text)
    value = db.Column(db.Text)


class Patch(DateModel):
    __tablename__ = "patch"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field = db.Column(db.Text, db.ForeignKey("field.field"))
    entry_number = db.Column(db.BigInteger)
    pattern = db.Column(db.Text)
    value = db.Column(db.Text)


class Skip(DateModel):
    __tablename__ = "skip"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    pattern = db.Column(db.Text)
    resource = db.Column(db.Text)


class Transform(DateModel):
    __tablename__ = "transform"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field = db.Column(db.Text, db.ForeignKey("field.field"))
    replacement_field = db.Column(db.Text)


class Filter(DateModel):
    __tablename__ = "filter"

    id = db.Column(db.Integer, primary_key=True)
    pipeline_id = db.Column(db.Text, db.ForeignKey("pipeline.pipeline"), nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    resource = db.Column(db.Text)
    field = db.Column(db.Text)
    pattern = db.Column(db.Text)


class SourceCheck:
    pass


class Collection:
    pass


class Resource:
    pass
