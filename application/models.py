import datetime
import uuid

from sqlalchemy.dialects.postgresql import ARRAY, JSON, UUID

from application.extensions import db


class DateModel(db.Model):

    __abstract__ = True

    entry_date = db.Column(db.TIMESTAMP)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)


class Organisation(DateModel):

    organisation = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text)
    official_name = db.Column(db.Text)
    addressbase_custodian = db.Column(db.Text)
    billing_authority = db.Column(db.Text)
    census_area = db.Column(db.Text)
    combined_authority = db.Column(db.Text)
    company = db.Column(db.Text)
    entity = db.Column(db.BigInteger)
    esd_inventory = db.Column(db.Text)
    local_authority_type = db.Column(db.Text)
    local_resilience_forum = db.Column(db.Text)
    opendatacommunities_area = db.Column(db.Text)
    opendatacommunities_organisation = db.Column(db.Text)
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

    def to_csv_dict(self):
        return {
            "organisation": self.organisation,
            "name": self.name,
            "addressbase-custodian": self.addressbase_custodian,
            "billing-authority": self.billing_authority,
            "census-area": self.census_area,
            "combined-authority": self.combined_authority,
            "company": self.company,
            "end-date": self.end_date,
            "entity": self.entity,
            "esd-inventories": self.esd_inventory,
            "local-authority-type": self.local_authority_type,
            "local-resilience-forum": self.local_resilience_forum,
            "opendatacommunities": self.opendatacommunities_organisation,
            "opendatacommunities-area": self.opendatacommunities_area,
            "region": self.region,
            "start-date": self.start_date,
            "statistical-geography": self.statistical_geography,
            "twitter": self.twitter,
            "website": self.website,
            "wikidata": self.wikidata,
            "wikipedia": self.wikipedia,
        }

    def __repr__(self):
        return f"<{self.__class__.__name__}> organisation: {self.organisation} entry_date: {self.entry_date}"


class Source(DateModel):

    source = db.Column(db.Text, primary_key=True, nullable=False)
    documentation_url = db.Column(db.Text)
    attribution = db.Column(db.Text)
    licence = db.Column(db.Text)
    endpoint_id = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"), nullable=True)
    organisation_id = db.Column(
        db.Text, db.ForeignKey("organisation.organisation"), nullable=True
    )
    collection_id = db.Column(
        db.Text, db.ForeignKey("collection.collection"), nullable=True
    )
    check = db.relationship("SourceCheck", back_populates="source", uselist=False)

    def to_dict(self):
        return {
            "source": self.source,
            "documentation-url": self.documentation_url,
            "endpoint": self.endpoint.endpoint if self.endpoint else None,
            "endpoint-url": self.endpoint.endpoint_url if self.endpoint else None,
            "organisation": self.organisation.organisation
            if self.organisation
            else None,
            "organisation_name": self.organisation.name if self.organisation else None,
            "licence": self.licence,
            "attribution": self.attribution,
            "collection": self.collection if self.collection else None,
            "entry-date": self.entry_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "start-date": self.start_date,
            "end-date": self.end_date,
            "datasets": self.datasets if self.datasets else None,
            "resources": [resource.resource for resource in self.endpoint.resources]
            if self.endpoint
            else None,
        }

    # Only needed until csv output for existing data processing no longer required
    def to_csv_dict(self):
        return {
            "source": self.source,
            "attribution": self.attribution,
            "collection": self.collection.collection if self.collection else None,
            "documentation-url": self.documentation_url,
            "endpoint": self.endpoint.endpoint if self.endpoint else None,
            "licence": self.licence,
            "organisation": self.organisation.organisation
            if self.organisation
            else None,
            "pipelines": ";".join([ds.dataset for ds in self.datasets])
            if self.datasets
            else None,
            "entry-date": self.entry_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "start-date": self.start_date,
            "end-date": self.end_date,
        }

    def update(self, data):
        for key, val in data.items():
            if hasattr(self, key) and key not in [
                "datasets",
                "organisation",
                "collection",
            ]:
                if val == "":
                    val = None
                setattr(self, key, val)


class Endpoint(DateModel):

    endpoint = db.Column(db.Text, primary_key=True, nullable=False)
    endpoint_url = db.Column(db.Text)
    parameters = db.Column(db.Text)
    plugin = db.Column(db.Text)
    sources = db.relationship(
        "Source", backref="endpoint", lazy=True, order_by="Source.entry_date"
    )

    def get_matching_source(self, organisation, dataset):
        for source in self.sources:
            if source.organisation.organisation == organisation:
                for ds in source.datasets:
                    if ds.dataset == dataset:
                        return source.source
        else:
            return None

    def to_dict(self):
        return {
            "endpoint": self.endpoint,
            "endpoint-url": self.endpoint_url,
            "parameters": self.parameters,
            "plugin": self.plugin,
            "sources": [s.to_dict() for s in self.sources],
            "entry-date": self.entry_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "start-date": self.start_date,
            "end-date": self.end_date,
        }

    def to_csv_dict(self):
        return {
            "endpoint": self.endpoint,
            "endpoint-url": self.endpoint_url,
            "parameters": self.parameters,
            "plugin": self.plugin,
            "entry-date": self.entry_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "start-date": self.start_date,
            "end-date": self.end_date,
        }


class Collection(DateModel):

    collection = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text)
    sources = db.relationship(
        "Source", backref="collection", lazy=True, order_by="Source.entry_date"
    )

    def to_dict(self):
        return {"collection": self.collection, "name": self.name}


source_dataset = db.Table(
    "source_dataset",
    db.Column("source", db.Text, db.ForeignKey("source.source"), primary_key=True),
    db.Column("dataset", db.Text, db.ForeignKey("dataset.dataset"), primary_key=True),
)


dataset_field = db.Table(
    "dataset_field",
    db.Column("dataset", db.Text, db.ForeignKey("dataset.dataset"), primary_key=True),
    db.Column("field", db.Text, db.ForeignKey("field.field"), primary_key=True),
    db.Column("entry_date", db.TIMESTAMP),
    db.Column("start_date", db.Date),
    db.Column("end_date", db.Date),
)


class Dataset(DateModel):

    dataset = db.Column(db.Text, primary_key=True, nullable=False)
    description = db.Column(db.Text)
    key_field = db.Column(db.Text)
    entity_minimum = db.Column(db.BigInteger)
    entity_maximum = db.Column(db.BigInteger)
    name = db.Column(db.Text)
    paint_options = db.Column(JSON)
    plural = db.Column(db.Text)
    prefix = db.Column(db.Text)
    text = db.Column(db.Text)
    wikidata = db.Column(db.Text)
    wikipedia = db.Column(db.Text)
    collection = db.Column(db.Text, db.ForeignKey("collection.collection"))
    typology = db.Column(db.Text, db.ForeignKey("typology.typology"))

    sources = db.relationship(
        "Source",
        secondary=source_dataset,
        lazy="subquery",
        backref=db.backref("datasets", lazy=True),
        order_by="Source.entry_date",
    )

    fields = db.relationship("Field", secondary=dataset_field, lazy="subquery")

    columns = db.relationship("Column", backref="dataset", lazy=True)
    concats = db.relationship("Concat", backref="dataset", lazy=True)
    defaults = db.relationship("Default", backref="dataset", lazy=True)

    def to_dict(self):
        return {
            "dataset": self.dataset,
            "description": self.description,
            "key-field": self.key_field,
            "entity-minimum": self.entity_minimum,
            "entity-maximum": self.entity_maximum,
            "name": self.name,
            "paint-options": self.paint_options,
            "plural": self.plural,
            "prefix": self.prefix,
            "text": self.text,
            "wikidata": self.wikidata,
            "wikipedia": self.wikipedia,
            "collection": self.collection,
            "typology": self.typology,
        }


class Typology(DateModel):

    typology = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text)
    description = db.Column(db.Text)
    text = db.Column(db.Text)
    plural = db.Column(db.Text)
    wikidata = db.Column(db.Text)
    wikipedia = db.Column(db.Text)
    fields = db.relationship("Field", backref="typology", lazy=True)


resource_endpoint = db.Table(
    "resource_endpoint",
    db.Column(
        "endpoint", db.Text, db.ForeignKey("endpoint.endpoint"), primary_key=True
    ),
    db.Column(
        "resource", db.Text, db.ForeignKey("resource.resource"), primary_key=True
    ),
)


class Resource(DateModel):

    resource = db.Column(db.Text, primary_key=True, nullable=False)
    mime_type = db.Column(db.Text)
    bytes = db.Column(db.Integer)

    endpoints = db.relationship(
        "Endpoint",
        secondary=resource_endpoint,
        lazy="subquery",
        backref=db.backref("resources", lazy=True),
    )

    columns = db.relationship("Column", backref="resource", lazy=True)

    def to_dict(self):
        return {
            "resource": self.resource,
            "endpoints": [e.to_dict() for e in self.endpoints],
            "mime-type": self.mime_type,
            "bytes": self.bytes,
        }


class Column(DateModel):

    column = db.Column(db.Text, primary_key=True, nullable=False)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"), nullable=True)
    resource_id = db.Column(db.Text, db.ForeignKey("resource.resource"), nullable=True)

    def to_csv_dict(self):
        return {
            "dataset": self.dataset_id,
            "resource": self.resource_id,
            "column": self.column,
            "field": self.field_id,
        }


class Datatype(DateModel):

    datatype = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text)
    text = db.Column(db.Text)
    fields = db.relationship("Field", backref="datatype", lazy=True)


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


class Default(DateModel):

    id = db.Column(db.Integer, primary_key=True)
    default_field = db.Column(db.Text)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"), nullable=True)
    resource_id = db.Column(db.Text, db.ForeignKey("resource.resource"), nullable=True)
    field = db.relationship("Field")
    resource = db.relationship("Resource")

    def to_csv_dict(self):
        return {
            "dataset": self.dataset_id,
            "resource": self.resource_id,
            "field": self.field_id,
            "default-field": self.default_field,
            "entry-date": self.entry_date,
            "start-date": self.start_date,
            "end-date": self.end_date,
        }


class Concat(DateModel):

    id = db.Column(db.Integer, primary_key=True)
    fields = db.Column(db.Text)
    separator = db.Column(db.Text)
    dataset_id = db.Column(db.Text, db.ForeignKey("dataset.dataset"), nullable=True)
    field_id = db.Column(db.Text, db.ForeignKey("field.field"), nullable=True)
    resource_id = db.Column(db.Text, db.ForeignKey("resource.resource"), nullable=True)
    field = db.relationship("Field")
    resource = db.relationship("Resource")

    def to_csv_dict(self):
        return {
            "dataset": self.dataset_id,
            "resource": self.resource_id,
            "field": self.field_id,
            "fields": self.fields,
            "separator": self.separator,
            "entry-date": self.entry_date,
            "start-date": self.start_date,
            "end-date": self.end_date,
        }


class SourceCheck(db.Model):

    id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_id = db.Column(db.Text, db.ForeignKey("source.source"))
    source = db.relationship("Source", back_populates="check")
    resource_hash = db.Column(db.Text)
    resource_rows = db.Column(JSON)
    resource_fields = db.Column(ARRAY(db.String))
    created_timestamp = db.Column(db.TIMESTAMP, default=datetime.datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "source": self.source if self.source else None,
            "resource": self.resource_hash,
            "rows": self.resource_rows,
            "fields": [field for field in self.resource_fields],
        }
