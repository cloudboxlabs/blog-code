# -*- encoding: utf-8 -*-
"""
Python Aplication Template
Licence: GPLv3
"""

from flask_sqlalchemy import SQLAlchemy

from app import app


db = SQLAlchemy(app)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
