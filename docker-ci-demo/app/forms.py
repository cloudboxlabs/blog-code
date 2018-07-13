from flask.ext.wtf import Form, TextField, TextAreaField, DateTimeField, PasswordField
from flask.ext.wtf import Required


class BlogPostForm(Form):
    title = TextField(u'Title', validators=[Required()])
    body = TextAreaField(u'Content')
    date = DateTimeField(u'Date', format='%Y-%m-%d')
    author = TextField(u'Author', validators=[Required()])

