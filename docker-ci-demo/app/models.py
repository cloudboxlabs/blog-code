from app import db


class Post(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(250))
    body = db.Column(db.Text)
    date = db.Column(db.DateTime)
    author = db.Column(db.String(50))
