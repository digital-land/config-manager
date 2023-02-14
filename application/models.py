# from sqlalchemy.dialects.postgresql import JSON

from application.extensions import db


class DateModel(db.Model):

    __abstract__ = True

    entry_date = db.Column(db.TIMESTAMP)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)


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


class Collection(db.Model):

    __tablename__ = "collection"

    collection = db.Column(db.Text, primary_key=True)
    name = db.Column(db.Text)


class Pipeline(db.Model):

    __tablename__ = "pipeline"

    pipeline = db.Column(db.Text, primary_key=True)
    name = db.Column(db.Text)
    source = db.relationship("Source", back_populates="pipelines")


class Source(DateModel):

    __tablename__ = "source"

    source = db.Column(db.Text, primary_key=True)
    attribution = db.Column(db.Text, db.ForeignKey("attribution.attribution"))
    collection = db.Column(db.Text, db.ForeignKey("collection.collection"))
    documentation_url = db.Column(db.Text)
    endpoint = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"))
    licence = db.Column(db.Text, db.ForeignKey("licence.licence"))
    organisation = db.Column(db.Text, db.ForeignKey("organisation.organisation"))
    pipelines = db.relationship("Pipeline", back_populates="source")


class Endpoint(DateModel):

    __tablename__ = "endpoint"

    endpoint = db.Column(db.Text, primary_key=True)
    endpoint_url = db.Column(db.Text)
    parameters = db.Column(db.Text)
    plugin = db.Column(db.Text)


# class Resource(DateModel):

#     __tablename__ = "resource"

#     resource = db.Column(db.Text, primary_key=True)
#     bytes = db.Column(db.BigInteger)
#     mime_type = db.Column(db.Text)


# class OldResource(DateModel):

#     __tablename__ = "old_resource"

#     old_resource = db.Column(db.Text, primary_key=True)
#     resource = db.Column(db.Text, db.ForeignKey("resource.resource"))
#     notes = db.Column(db.Text)
#     status = db.Column(db.Text)


# class Dataset(DateModel):

#     __tablename__ = "dataset"

#     dataset = db.Column(db.Text, primary_key=True)
#     attribution = db.Column(db.Text, db.ForeignKey("attribution.attribution"))
#     collection = db.Column(db.Text, db.ForeignKey("collection.collection"))
#     description = db.Column(db.Text)
#     key_field = db.Column(db.Text)
#     entity_minimum = db.Column(db.BigInteger)
#     entity_maximum = db.Column(db.BigInteger)
#     licence = db.Column(db.Text, db.ForeignKey("licence.licence"))
#     name = db.Column(db.Text)
#     paint_options = db.Column(JSON)
#     plural = db.Column(db.Text)
#     prefix = db.Column(db.Text)
#     text = db.Column(db.Text)
#     typology = db.Column(db.Text, db.ForeignKey("typology.typology"))
#     wikidata = db.Column(db.Text)
#     wikipedia = db.Column(db.Text)


class Attribution(DateModel):

    __tablename__ = "attribution"

    attribution = db.Column(db.Text, primary_key=True)
    text = db.Column(db.Text)


class Licence(DateModel):

    __tablename__ = "licence"

    licence = db.Column(db.Text, primary_key=True)
    text = db.Column(db.Text)


class Column(db.Model):

    __tablename__ = "column"

    id = db.Column(db.Integer, primary_key=True)
    column = db.Column(db.Text)
    resource = db.Column(db.Text, db.ForeignKey("resource.resource"))
    dataset = db.Column(db.Text, db.ForeignKey("dataset.dataset"))
    field = db.Column(db.Text, db.ForeignKey("field.field"))


class Combine(DateModel):

    __tablename__ = "combine"

    rowid = db.Column(db.Integer, primary_key=True)
    field = db.Column(db.Text, db.ForeignKey("field.field"))
    dataset = db.Column(db.Text, db.ForeignKey("dataset.dataset"))
    resource = db.Column(db.Text, db.ForeignKey("resource.resource"))
    separator = db.Column(db.Text)


class Concat(DateModel):

    __tablename__ = "concat"

    rowid = db.Column(db.Integer, primary_key=True)
    field = db.Column(db.Text, db.ForeignKey("field.field"))
    dataset = db.Column(db.Text, db.ForeignKey("dataset.dataset"))
    resource = db.Column(db.Text, db.ForeignKey("resource.resource"))
    fields = db.Column(db.Text)
    separator = db.Column(db.Text)


class Convert(DateModel):

    __tablename__ = "convert"

    rowid = db.Column(db.Integer, primary_key=True)
    dataset = db.Column(db.Text, db.ForeignKey("dataset.dataset"))
    resource = db.Column(db.Text, db.ForeignKey("resource.resource"))
    plugin = db.Column(db.Text)


class Default(DateModel):

    __tablename__ = "_default"

    rowid = db.Column(db.Integer, primary_key=True)
    field = db.Column(db.Text, db.ForeignKey("field.field"))
    dataset = db.Column(db.Text, db.ForeignKey("dataset.dataset"))
    resource = db.Column(db.Text, db.ForeignKey("resource.resource"))
    default_field = db.Column(db.Text)


class DefaultValue(DateModel):

    __tablename__ = "default_value"

    rowid = db.Column(db.Integer, primary_key=True)
    endpoint = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"))
    field = db.Column(db.Text, db.ForeignKey("field.field"))
    dataset = db.Column(db.Text, db.ForeignKey("dataset.dataset"))
    resource = db.Column(db.Text, db.ForeignKey("resource.resource"))
    entry_number = db.Column(db.BigInteger)
    value = db.Column(db.Text)


class Patch(DateModel):

    __tablename__ = "patch"

    rowid = db.Column(db.Integer, primary_key=True)
    field = db.Column(db.Text, db.ForeignKey("field.field"))
    dataset = db.Column(db.Text, db.ForeignKey("dataset.dataset"))
    resource = db.Column(db.Text, db.ForeignKey("resource.resource"))
    entry_number = db.Column(db.BigInteger)
    pattern = db.Column(db.Text)
    value = db.Column(db.Text)


class Skip(DateModel):

    __tablename__ = "skip"

    rowid = db.Column(db.Integer, primary_key=True)
    dataset = db.Column(db.Text, db.ForeignKey("dataset.dataset"))
    pattern = db.Column(db.Text)
    resource = db.Column(db.Text)


class Transform(DateModel):

    __tablename__ = "transform"

    rowid = db.Column(db.Integer, primary_key=True)
    field = db.Column(db.Text, db.ForeignKey("field.field"))
    dataset = db.Column(db.Text, db.ForeignKey("dataset.dataset"))
    replacement_field = db.Column(db.Text)


class Filter(DateModel):

    __tablename__ = "filter"

    rowid = db.Column(db.Integer, primary_key=True)
    field = db.Column(db.Text)
    dataset = db.Column(db.Text)
    pattern = db.Column(db.Text)
