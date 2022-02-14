from application.extensions import db


class Organisation(db.Model):

    organisation = db.Column(db.TEXT, primary_key=True, nullable=False)
    name = db.Column(db.TEXT)
    official_name = db.Column(db.TEXT)
    addressbase_custodian = db.Column(db.TEXT)
    billing_authority = db.Column(db.TEXT)
    census_area = db.Column(db.TEXT)
    combined_authority = db.Column(db.TEXT)
    company = db.Column(db.TEXT)
    entity = db.Column(db.BIGINT)
    esd_inventory = db.Column(db.TEXT)
    local_authority_type = db.Column(db.TEXT)
    local_resilience_forum = db.Column(db.TEXT)
    opendatacommunities_area = db.Column(db.TEXT)
    opendatacommunities_organisation = db.Column(db.TEXT)
    region = db.Column(db.TEXT)
    shielding_hub = db.Column(db.TEXT)
    statistical_geography = db.Column(db.TEXT)
    twitter = db.Column(db.TEXT)
    website = db.Column(db.TEXT)
    wikidata = db.Column(db.TEXT)
    wikipedia = db.Column(db.TEXT)
    entry_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    start_date = db.Column(db.Date)
    sources = db.relationship("Source", backref="organisation", lazy=True)

    def __repr__(self):
        return f"<{self.__class__.__name__}> organisation: {self.organisation} entry_date: {self.entry_date}"


class Source(db.Model):

    source = db.Column(db.TEXT, primary_key=True, nullable=False)
    documentation_url = db.Column(db.TEXT)
    attribution = db.Column(db.TEXT)
    licence = db.Column(db.TEXT)

    entry_date = db.Column(db.Date)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)

    endpoint_id = db.Column(db.TEXT, db.ForeignKey("endpoint.endpoint"), nullable=False)
    organisation_id = db.Column(
        db.TEXT, db.ForeignKey("organisation.organisation"), nullable=False
    )
    # TODO add collection


class Endpoint(db.Model):

    endpoint = db.Column(db.TEXT, primary_key=True, nullable=False)
    endpoint_url = db.Column(db.TEXT, nullable=False)
    parameters = db.Column(db.TEXT)
    plugin = db.Column(db.TEXT)
    entry_date = db.Column(db.Date)
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    sources = db.relationship("Source", backref="endpoint", lazy=True)
