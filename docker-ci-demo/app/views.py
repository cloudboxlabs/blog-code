import os

from flask import render_template, request
import redis

from app import app, db
from app.models import Post
from forms import BlogPostForm


redis_client = redis.StrictRedis(host=os.getenv('REDIS_HOST'), port=6379)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/new/')
def new():
    form = BlogPostForm()
    return render_template('new.html', form=form)


@app.route('/save/', methods=['GET', 'POST'])
def save():
    form = BlogPostForm()
    if form.validate_on_submit():
        if request.form['action'] == 'draft':
            print('Saving to redis')
            redis_client.set(form.title.data, form.body.data)
        else:
            print('Saving to postgres')
            model = Post()
            model.title = form.title.data
            model.body = form.body.data
            model.date = form.date.data
            model.author = form.author.data
            db.session.add(model)
            db.session.commit()
    return render_template('new.html', form=form)


@app.route('/view/<id>/')
def view(id):
    return render_template('view.html')


