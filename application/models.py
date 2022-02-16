import datetime
from dataclasses import dataclass

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

    def __repr__(self):
        return f"<{self.__class__.__name__}> organisation: {self.organisation} entry_date: {self.entry_date}"


@dataclass
class Source(DateModel):

    source: str
    documentation_url: str
    attribution: str
    licence: str
    entry_date: datetime.date
    start_date: datetime.date
    end_date: datetime.date
    endpoint: str
    organisation: str
    collection: str

    source = db.Column(db.Text, primary_key=True, nullable=False)
    documentation_url = db.Column(db.Text)
    attribution = db.Column(db.Text)
    licence = db.Column(db.Text)
    endpoint = db.Column(db.Text, db.ForeignKey("endpoint.endpoint"))
    organisation = db.Column(db.Text, db.ForeignKey("organisation.organisation"))
    collection = db.Column(db.Text)


@dataclass
class Endpoint(DateModel):

    endpoint: str
    endpoint_url: str
    parameters: str
    plugin: str
    entry_date: datetime.date
    start_date: datetime.date
    end_date: datetime.date
    sources: list

    endpoint = db.Column(db.Text, primary_key=True, nullable=False)
    endpoint_url = db.Column(db.Text)
    parameters = db.Column(db.Text)
    plugin = db.Column(db.Text)
    sources = db.relationship("Source", lazy=True)


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
