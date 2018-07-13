
class Config(object):
    """
    Configuration base, for all environments.
    """
    DEBUG = False
    TESTING = False
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://postgres@localhost:5432/postgres'
    BOOTSTRAP_FONTAWESOME = True
    SECRET_KEY = "MINHACHAVESECRETA"
    CSRF_ENABLED = True


class ProductionConfig(Config):
    DATABASE_URI = 'mysql://user@localhost/foo'


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    SQLALCHEMY_DATABASE_URI = 'postgresql+psycopg2://postgres@postgres:5432/postgres'
    TESTING = True
    CSRF_ENABLED = False


config = {"dev": DevelopmentConfig, "prod": ProductionConfig, "docker": TestingConfig}