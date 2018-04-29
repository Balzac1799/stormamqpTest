# encoding: utf-8

import time
import logging

import amqpstorm
from amqpstorm import Connection
from amqpstorm import Message


logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger()


def publish_message():
    with Connection('127.0.0.1', 'guest', 'guest') as connection:
        with connection.channel() as channel:
            channel.queue.declare('simple_queue')

            # Message Properties.
            properties = {
                'content_type': 'text/plain',
                'headers': {'key': 'value'}
            }

            # Create the message.
            message = Message.create(channel, 'Hello World!', properties)

            # Publish the message to a queue called, 'simple_queue'.
            print('Connection is ', connection)
            message.publish('simple_queue')


class Producer(object):
    def __init__(self, max_retries=None):
        self.max_retries = max_retries
        self.connection = None

    def create_connection(self):
        """Create a connection.

        :return:
        """
        attempts = 0
        while True:
            attempts += 1
            try:
                self.connection = Connection('127.0.0.1', 'guest', 'guest')
                break
            except amqpstorm.AMQPError as why:
                LOGGER.exception(why)
                if self.max_retries and attempts > self.max_retries:
                    break
                time.sleep(min(attempts * 2, 30))
            except KeyboardInterrupt:
                break

    def pub_msg(self, message):
        """Start the Consumers.

        :return:
        """
        if not self.connection:
            self.create_connection()

        try:
            # if message == '50':
            #     self.connection.close()
            self._pub_msg(message)

        except amqpstorm.AMQPError as why:
            print('Start reconnect')
            LOGGER.exception(why)
            self.create_connection()
            self._pub_msg(message)

        except KeyboardInterrupt:
            self.connection.close()

    def _pub_msg(self, message):
        channel = self.connection.channel()
        channel.queue.declare('log.develop', durable=True)
        # Message Properties.
        properties = {
            'content_type': 'text/plain',
            'headers': {'key': 'value'}
        }

        # Create the message.
        print('Connection is', self.connection, message)
        message = Message.create(channel, message, properties)
        message.publish('log.develop', exchange='')
        channel.close()


class CeleryProducer(object):
    def __init__(self, **kwargs):
        self.max_retries = kwargs.get('max_retries', 3)
        self.connection = None
        self.host = kwargs.get('host', '127.0.0.1')
        self.port = kwargs.get('port', 5672)
        self.vhost = kwargs.get('vhost', '/')
        self.user = kwargs.get('user', 'guest')
        self.password = kwargs.get('password', 'guest')
        self.exchange = kwargs.get('exchange', '')
        self.exchange_type = kwargs.get('exchange_type', 'direct')
        self.tms_queue = kwargs.get('tms_queue', '')
        self.account_queue = kwargs.get('account_queue', '')
        self.routing_key = kwargs.get('routing_key', 'no-routing-key')

    def create_connection(self):
        """Create a connection.

        :return:
        """
        attempts = 0
        while True:
            attempts += 1
            try:
                self.connection = Connection(self.host, self.user,
                                             self.password)
                break
            except amqpstorm.AMQPError as why:
                LOGGER.exception(why)
                if self.max_retries and attempts > self.max_retries:
                    break
                time.sleep(min(attempts * 2, 30))
            except KeyboardInterrupt:
                break

    def pub_msg(self, message):
        """Start the Consumers.

        :return:
        """
        if not self.connection:
            self.create_connection()
        try:
            self._pub_msg(message)
        except amqpstorm.AMQPError as why:
            LOGGER.info('Start reconnect')
            LOGGER.exception(why)
            self.create_connection()
            self._pub_msg(message)

        except KeyboardInterrupt:
            self.connection.close()

    def _pub_msg(self, message):
        channel = self.connection.channel()
        # Message Properties.
        properties = {
            'content_type': 'text/plain',
            'delivery_mode': 2
        }

        # Create the message.
        print('Connection is', self.connection, message)
        message = Message.create(channel, message, properties)
        message.publish(self.routing_key, exchange=self.exchange)
        channel.close()

    def setup(self):
        """
        Setup queue and exchange
        """
        import threading
        self.create_connection()
        channel = self.connection.channel()
        channel.exchange.declare(
            exchange=self.exchange, exchange_type=self.exchange_type)
        channel.queue.declare(self.tms_queue, durable=True)
        channel.queue.declare(self.account_queue, durable=True)
        channel.queue.bind(
            queue=self.tms_queue,
            exchange=self.exchange,
            routing_key=self.routing_key)
        channel.queue.bind(
            queue=self.account_queue,
            exchange=self.exchange,
            routing_key=self.routing_key)
        channel.close()
