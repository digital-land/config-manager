from datetime import datetime

from application.extensions import db


class ServiceLock(db.Model):
    __tablename__ = "service_lock"

    name = db.Column(db.Text, primary_key=True)
    locked_by = db.Column(db.Text, nullable=False)
    locked_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class RequestMeta(db.Model):
    __tablename__ = "request_meta"

    request_id = db.Column(db.Text, primary_key=True)
    endpoints_to_retire = db.Column(
        db.Text, nullable=True
    )  # JSON list of endpoint hashes
    endpoints_to_unretire = db.Column(
        db.Text, nullable=True
    )  # JSON list of endpoint hashes to clear the end-date for
    branch_sha = db.Column(
        db.Text, nullable=True
    )  # config-manager-update HEAD SHA when the assessment was submitted
    check_request_id = db.Column(
        db.Text, nullable=True
    )  # check-results request this assessment came from, for re-run routing
    source_flow = db.Column(
        db.Text, nullable=True
    )  # "add_data" or "assign_entities" - which flow created this request, so the
    # shared preview/confirm pages apply the correct process lock
    dispatched_at = db.Column(
        db.DateTime, nullable=True
    )  # set atomically when the confirm dispatches, so a retry can't submit twice


class EntityClaim(db.Model):
    __tablename__ = "entity_claim"

    id = db.Column(db.Integer, primary_key=True)
    collection = db.Column(db.Text, nullable=False)
    entity = db.Column(db.BigInteger, nullable=False)
    branch = db.Column(db.Text, nullable=False)
    request_id = db.Column(db.Text, nullable=False)
    claimed_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        db.UniqueConstraint(
            "collection",
            "entity",
            "branch",
            name="uq_entity_claim_collection_entity_branch",
        ),
    )
