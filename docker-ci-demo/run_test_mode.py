import time

from app import app, db
from app.models import Post


if __name__ == "__main__":
    # wait for postgres to be ready
    time.sleep(5)
    db.create_all()

    app.run(host='0.0.0.0', port=5000)
