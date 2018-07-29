FROM python:2.7

# Install packages
RUN set -ex; \
    apt-get update; \
    apt-get -y -qq install postgresql redis-tools

ADD . /app
WORKDIR /app

RUN pip install -r requirements.txt

