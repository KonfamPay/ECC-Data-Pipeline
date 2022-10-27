FROM debezium/connect:1.4
USER root:root
COPY ./plugins/ /opt/kafka/plugins/
ENV CONNECT_PLUGIN_PATH="/opt/kafka/plugins"
USER 1001