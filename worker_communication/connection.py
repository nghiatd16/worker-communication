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
        "RABBITMQ_USER": "zalo", # Username of account
        "RABBITMQ_PWD": "zalo" # Password of account
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

        # Set up connection
        self._init_connection_and_channel()

    def __exit__(self):
        try:
            self.consume_channel.stop_consuming()
        except:
            pass
        try:
            self.produce_channel.close()
        except:
            pass
        try:
            self.consume_channel.close()
        except:
            pass
        try:
            self.connection.close()
        except:
            pass
        print("Exit function called")
    
    def __del__(self):
        try:
            self.consume_channel.stop_consuming()
        except:
            pass
        try:
            self.produce_channel.close()
        except:
            pass
        try:
            self.consume_channel.close()
        except:
            pass
        try:
            self.connection.close()
        except:
            pass
        print("Del function called")

    def _init_connection_and_channel(self):
        # Set up connection
        self.connection = pika.BlockingConnection(self.conn_params)
        # Set up channel
        self.produce_channel = self.connection.channel()
        self.consume_channel = self.connection.channel()
        # Fair dispatch. More detail read Fair dispatch in https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        self.produce_channel.basic_qos(prefetch_count=1)
        self.consume_channel.basic_qos(prefetch_count=1)

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

    def _reconnect(self, num_tries=None, delay_time=5):
        counter = 0
        while num_tries is None or counter < num_tries:
            try:
                counter += 1
                self._init_connection_and_channel()
                return True
            except:
                logging.info("Reconnect failed. Tried {}/{}.".format(counter, num_tries if num_tries is not None else "INF"))
                if num_tries is None or counter < num_tries:
                    logging.info("Trying to reconnect in {} second(s)".format(delay_time))
                    time.sleep(delay_time)
        return False

    def _execute_function_in_ensure_connection(self, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except AMQPConnectionError:
            logging.exception("Lost connection. Reconnecting ...")
            stt = self._reconnect()
            if not stt:
                raise ConnectionError("Cannot reconnect to RabbitMQ Service")