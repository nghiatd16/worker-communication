import pika
from pika.exceptions import AMQPConnectionError
import copy
import logging
import time
from abc import abstractmethod

class BaseConnector:
    DEFAULT_CONFIG = {
        "RABBITMQ_HOST": "127.0.0.1", # IP Address or Domain name of RabbitMQ Service
        "RABBITMQ_PORT": 5672, # Serving port of RabbitMQ Service
        "RABBITMQ_USER": "guest", # Username of account
        "RABBITMQ_PWD": "guest" # Password of account
    }
    INDICATOR = {
        "current_task_name": "__indicator__current_task_name",
        "current_task_status": "__indicator__current_task_status",
        "current_task_result": "__indicator__current_task_result",
        "current_task_error": "__indicator__current_task_error"
    }

    def __init__(self, **configs):
        
        # update custom configs
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)
        
        # Only check for extra config keys
        assert not configs, 'Unrecognized configs: %s' % (configs,)

        # Create connection parameters
        self.conn_params = self._create_rabbitmq_connection_parameters()

    def get_queue_size(self, queue_name):
        connection = pika.BlockingConnection(self.conn_params)
        channel = connection.channel()
        q = channel.queue_declare(queue=queue_name, passive=True)
        queue_size = q.method.message_count
        channel.close()
        connection.close()
        return queue_size

    def get_produce_instance(self):
        connection = pika.BlockingConnection(self.conn_params)
        produce_channel = connection.channel()
        produce_channel.basic_qos(prefetch_count=1)
        return produce_channel
    
    def get_consume_instance(self):
        connection = pika.BlockingConnection(self.conn_params)
        # Set up channel
        consume_channel = connection.channel()
        # Fair dispatch. More detail read Fair dispatch in https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        consume_channel.basic_qos(prefetch_count=1)
        return consume_channel

    def _create_rabbitmq_connection_parameters(self):

        # Authentication connection
        credentials = pika.PlainCredentials(
            self.config['RABBITMQ_USER'], 
            self.config['RABBITMQ_PWD']
        )

        # Create connection parameter. Required by RabbitMQ
        conn_params = pika.ConnectionParameters(
            host=self.config['RABBITMQ_HOST'],
            port=self.config['RABBITMQ_PORT'],
            credentials=credentials
        )
        return conn_params