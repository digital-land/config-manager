from authlib.integrations.flask_client import OAuth
from flask_caching import Cache
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from flask_talisman import Talisman

cache = Cache()
db = SQLAlchemy()
migrate = Migrate(db=db)
oauth = OAuth()
talisman = Talisman()
