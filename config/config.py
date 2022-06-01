# -*- coding: utf-8 -*-
import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    APP_ROOT = os.path.abspath(os.path.dirname(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(APP_ROOT, os.pardir))
    SECRET_KEY = os.getenv("SECRET_KEY")
    DATABASE_URL = os.getenv("DATABASE_URL")
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://")
    SQLALCHEMY_DATABASE_URI = DATABASE_URL
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    GITHUB_CLIENT_ID = os.getenv("GITHUB_CLIENT_ID")
    GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET")
    SAFE_URLS = {"data-manager-prototype.herokuapp.com"}
    AUTHENTICATION_ON = True
    S3_BUCKET_URL = (
        "https://digital-land-production-collection-dataset.s3.eu-west-2.amazonaws.com"
    )


class DevelopmentConfig(Config):
    DEBUG = True
    WTF_CSRF_ENABLED = False
    SAFE_URLS = {"localhost:80"}
    AUTHENTICATION_ON = False


class TestConfig(Config):
    ENV = "test"
    DEBUG = True
    TESTING = True
    AUTHENTICATION_ON = False
