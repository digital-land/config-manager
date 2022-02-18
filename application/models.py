from sqlalchemy.dialects.postgresql import JSON

from application.extensions import db


class DateModel(db.Model):

    __abstract__ = True

    _entry_date = db.Column(db.Date)
    _start_date = db.Column(db.Date)
    _end_date = db.Column(db.Date)

    @property
    def entry_date(self):
        return self._entry_date.isoformat() if self._entry_date else None

    @entry_date.setter
    def entry_date(self, entry_date):
        self._entry_date = entry_date

    @property
    def start_date(self):
        return self._start_date.isoformat() if self._start_date else None

    @start_date.setter
    def start_date(self, start_date):
        self._start_date = start_date

    @property
    def end_date(self):
        return self._end_date.isoformat() if self._end_date else None

    @end_date.setter
    def end_date(self, end_date):
        self._end_date = end_date


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
    sources = db.relationship("Source", backref="organisation", lazy=True)

    def __repr__(self):
        return f"<{self.__class__.__name__}> organisation: {self.organisation} entry_date: {self.entry_date}"


class Source(DateModel):

    source = db.Column(db.Text, primary_key=True, nullable=False)
    documentation_url = db.Column(db.Text)
    attribution = db.Column(db.Text)
    licence = db.Column(db.Text)
    _endpoint = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"))
    _organisation = db.Column(db.Text, db.ForeignKey("organisation.organisation"))
    collection = db.Column(db.Text)

    @property
    def endpoint(self):
        return self._endpoint

    @endpoint.setter
    def endpoint(self, endpoint):
        self._endpoint = endpoint

    @property
    def organisation(self):
        return self._organisation

    @organisation.setter
    def organisation(self, organisation):
        self._organisation = organisation

    def to_dict(self):
        return {
            "source": self.source,
            "documentation_url": self.documentation_url,
            "endpoint": self.endpoint.endpoint,
            "endpoint_url": self.endpoint.endpoint_url,
            "organisation": self.organisation.organisation,
            "organisation_name": self.organisation.name,
            "licence": self.licence,
            "collection": self.collection,
            "entry_date": self.entry_date,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "pipelines": self.pipelines,
        }


class Endpoint(DateModel):

    endpoint = db.Column(db.Text, primary_key=True, nullable=False)
    endpoint_url = db.Column(db.Text)
    parameters = db.Column(db.Text)
    plugin = db.Column(db.Text)
    sources = db.relationship("Source", backref="endpoint", lazy=True)

    def to_dict(self):
        return {
            "endpoint": self.endpoint,
            "endpoint_url": self.endpoint_url,
            "parameters": self.parameters,
            "plugin": self.plugin,
            "sources": [s.to_dict() for s in self.sources],
            "entry_date": self.entry_date,
            "start_date": self.start_date,
            "end_date": self.end_date,
        }


class Collection(DateModel):

    collection = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text)


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
    typology = db.Column(db.Text)
    wikidata = db.Column(db.Text)
    wikipedia = db.Column(db.Text)
    collection = db.Column(db.Text, db.ForeignKey("collection.collection"))
    typology = db.Column(db.Text, db.ForeignKey("typology.typology"))


class Typology(DateModel):

    typology = db.Column(db.Text, primary_key=True, nullable=False)
    name = db.Column(db.Text)
    description = db.Column(db.Text)
    text = db.Column(db.Text)
    plural = db.Column(db.Text)
    wikidata = db.Column(db.Text)
    wikipedia = db.Column(db.Text)


resource_endpoint = db.Table(
    "resource_endpoint",
    db.Column(
        "endpoint", db.Text, db.ForeignKey("endpoint.endpoint"), primary_key=True
    ),
    db.Column(
        "resource", db.Text, db.ForeignKey("resource.resource"), primary_key=True
    ),
)


source_pipeline = db.Table(
    "source_pipeline",
    db.Column("source", db.Text, db.ForeignKey("source.source"), primary_key=True),
    db.Column(
        "pipeline", db.Text, db.ForeignKey("pipeline.pipeline"), primary_key=True
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


class Pipeline(DateModel):
    pipeline = db.Column(db.Text, primary_key=True, nullable=False)
    schema = db.Column(db.Text)

    sources = db.relationship(
        "Source",
        secondary=source_pipeline,
        lazy="subquery",
        backref=db.backref("pipelines", lazy=True),
    )

    def to_dict(self):
        return {
            "pipeline": self.pipeline,
            "schema": self.schema,
        }
