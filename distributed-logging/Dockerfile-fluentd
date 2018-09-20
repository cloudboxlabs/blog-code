FROM fluent/fluentd:v0.12-debian

ENV FLUENT_UID=0
RUN mkdir /buffer
RUN ["gem", "install", "fluent-plugin-kafka", "--no-rdoc", "--no-ri", "--version", "0.7.9"]
