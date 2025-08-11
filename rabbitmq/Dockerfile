FROM rabbitmq:3.11-management

RUN apt-get update && apt-get install -y curl

RUN rabbitmq-plugins enable --offline rabbitmq_management


RUN curl -L https://github.com/rabbitmq/rabbitmq-delayed-message-exchange/releases/download/3.11.1/rabbitmq_delayed_message_exchange-3.11.1.ez -o /plugins/rabbitmq_delayed_message_exchange.ez

ENV RABBITMQ_PLUGINS_DIR="/plugins"

RUN rabbitmq-plugins enable --offline rabbitmq_delayed_message_exchange

COPY rabbitmq.conf /etc/rabbitmq/rabbitmq.conf

EXPOSE 5672 15672
