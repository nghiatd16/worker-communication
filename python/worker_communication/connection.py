import pika
from pika.exceptions import AMQPConnectionError
import copy
import logging
import time
from abc import abstractmethod
import random
from retry import retry

class ClusterConnectionParams:
    DEFAULT_CONFIG = {
        "RABBITMQ_HOST": "127.0.0.1", # IP Address or Domain name of RabbitMQ Service
        "RABBITMQ_PORT": 5672, # Serving port of RabbitMQ Service
        "RABBITMQ_USER": "guest", # Username of account
        "RABBITMQ_PWD": "guest" # Password of account
    }

    def __init__(self, **configs):
        
        # update custom configs
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)
        
        # Only check for extra config keys
        assert not configs, 'Unrecognized configs: %s' % (configs,)

        self.HOST = self.config['RABBITMQ_HOST']
        self.PORT = self.config['RABBITMQ_PORT']
        self.USER = self.config['RABBITMQ_USER']
        self.PWD = self.config['RABBITMQ_PWD']
    
    def __str__(self):
        return str(self.config)

class BaseConnector:
    INDICATOR = {
        "current_task_name": "__indicator__current_task_name",
        "current_task_status": "__indicator__current_task_status",
        "current_task_result": "__indicator__current_task_result",
        "current_task_error": "__indicator__current_task_error"
    }

    def __init__(self, connection_params_list=None):
        if connection_params_list is None:
            connection_params_list = [ClusterConnectionParams()]
        # Set up all node of cluster connection params
        self.all_endpoints = list()
        for p in connection_params_list:
            self.all_endpoints.append(self._create_rabbitmq_connection_parameters(p))
    
    def _random_node(self):
        random.shuffle(self.all_endpoints)

    @retry(AMQPConnectionError, tries=-1, delay=1, jitter=1)
    def get_queue_size(self, queue_name):
        self._random_node()
        connection = pika.BlockingConnection(self.all_endpoints)
        channel = connection.channel()
        q = channel.queue_declare(queue=queue_name, passive=True)
        queue_size = q.method.message_count
        channel.close()
        connection.close()
        return queue_size

    @retry(AMQPConnectionError, tries=-1, delay=1, jitter=1)
    def get_produce_instance(self):
        self._random_node()
        connection = pika.BlockingConnection(self.all_endpoints)
        produce_channel = connection.channel()
        produce_channel.basic_qos(prefetch_count=1)
        return produce_channel
    
    @retry(AMQPConnectionError, tries=-1, delay=1, jitter=1)
    def get_consume_instance(self):
        self._random_node()
        connection = pika.BlockingConnection(self.all_endpoints)
        # Set up channel
        consume_channel = connection.channel()
        # Fair dispatch. More detail read Fair dispatch in https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        consume_channel.basic_qos(prefetch_count=1)
        return consume_channel

    def _create_rabbitmq_connection_parameters(self, connection_params):

        # Authentication connection
        credentials = pika.PlainCredentials(
            connection_params.config['RABBITMQ_USER'], 
            connection_params.config['RABBITMQ_PWD']
        )

        # Create connection parameter. Required by RabbitMQ
        conn_params = pika.ConnectionParameters(
            host=connection_params.config['RABBITMQ_HOST'],
            port=connection_params.config['RABBITMQ_PORT'],
            credentials=credentials
        )
        return conn_params