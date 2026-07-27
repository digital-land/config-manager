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
    branch_sha = db.Column(
        db.Text, nullable=True
    )  # config-manager-update HEAD SHA when the assessment was submitted
    check_request_id = db.Column(
        db.Text, nullable=True
    )  # check-results request this assessment came from, for re-run routing
