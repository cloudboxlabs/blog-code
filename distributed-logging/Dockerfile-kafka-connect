FROM confluentinc/cp-kafka-connect:4.0.0

RUN apt-get update && apt-get install -y vim

RUN mkdir -p /opt
RUN mkdir -p /opt/connectors
ADD connector_jars /opt/connectors/connector_jars
WORKDIR /opt

ADD connector_conf/ /opt/connector_conf
RUN wget https://github.com/confluentinc/kafka-connect-elasticsearch/archive/v3.3.3-rc1.tar.gz; \
    tar xzf v3.3.3-rc1.tar.gz -C /opt/connectors
