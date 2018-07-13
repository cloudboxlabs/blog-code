# -*- encoding: utf-8 -*-
"""
Python Aplication Template
Licence: GPLv3
"""
import os

from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.login import LoginManager

from app.configuration import config

app = Flask(__name__)

env = os.environ.get("FLASK_ENV", "dev")
app.config.from_object(config[env])

db = SQLAlchemy(app)

lm = LoginManager()
lm.setup_app(app)
lm.login_view = 'login'

from app import views, models
