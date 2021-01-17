import pika
from abc import abstractmethod, ABC
from .connection import BaseConnector
from .job_description import JobDescription
import traceback
import time
import logging
class BaseWorker(BaseConnector, ABC):
    LEAF_WORKER_NAME = "LEAF-WORKER"
    DEAD_LETTER_WORKER_NAME = "DEAD-LETTER-QUEUE"
    def __init__(self, production_key, worker_name, consume_queue_name=None, produce_queue_name=None, **configs):
        '''
            Base Worker defines the criterion of worker including necessary attributes, required functions, ...
            You shouldn't inherit BaseWorker because it requires much work to do, 
            considering RootWorker, LeafWorker, AbstractWorker, DeadLetterWorker for various specific purposes.
            Args:
                production_key: An unique string for each production
                consume_queue_name: A name of queue that worker will receive job. If worker is root, consume_queue should be None, 
                                because root worker only push a jobDescription to queue. It will not do anything logic.
                produce_queue_name: A name of queue that worker will produce jobDescription after complete its job. If worker is leaf,
                                produce_queue should be None, because leaf worker only do some procedures to finish the pipeline, example:
                                update status, response users, write logs, etc ...
        '''
        super().__init__(**configs)
        if consume_queue_name is None and produce_queue_name is None:
            if (worker_name not in ['LEAF-WORKER', 'DEAD-LETTER-WORKER']):
                raise Exception("Consume and Produce Queue have both None")
        self.production_key = production_key
        self.worker_name = worker_name
        self.dead_letter_queue = self.getDeadWorkerQueueName(self.production_key)
        if consume_queue_name is not None:
            self.consume_queue = "{}__{}".format(self.production_key, consume_queue_name)
        elif worker_name == "DEAD-LETTER-WORKER":
            self.consume_queue = self.dead_letter_queue
        elif worker_name == "LEAF-WORKER":
            self.consume_queue = self.getLeafWorkerQueueName(self.production_key)
        else:
            self.consume_queue = None
        if produce_queue_name is not None:
            self.produce_queue = "{}__{}".format(self.production_key, produce_queue_name)
        else:
            self.produce_queue = None
        
        # Define Dead-Letter-Queue
        self.consume_channel = self.get_consume_instance()
        self.consume_channel.queue_declare(self.dead_letter_queue, durable=True)

        self._declare_queues()
    
    def sendMessageToDeadLetter(self, job_description):
        dead_worker_name = self.getDeadWorkerQueueName(self.production_key)
        produce_channel = self.get_produce_instance()
        produce_channel.basic_publish(
                exchange='',
                routing_key=self.dead_letter_queue,
                body=job_description.to_json(),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
            ))
        produce_channel.close()
        produce_channel.connection.close()

    @classmethod
    def getLeafWorkerQueueName(cls, production_key):
        return "{}__LEAF-WORKER".format(production_key)
    
    @classmethod
    def getDeadWorkerQueueName(cls, production_key):
        return "{}__DEAD-LETTER-QUEUE".format(production_key)

    @abstractmethod
    def _declare_queues(self):
        '''
            Method init all necessary queues.
        '''
        pass

    def _declare_queue(self, queue_name, message_ttl=3600000, dead_letter_exchange=None, dead_letter_routing_key=None):
        '''
            Method declare queue with provided queue_name
        '''
        if queue_name is not None:
            self.consume_channel.queue_declare(queue=queue_name, durable=True, arguments={
                                            'x-message-ttl' : message_ttl, # Time-to-live message, default 3 600 000ms = 3600s
                                            'x-dead-letter-exchange' : '' if dead_letter_exchange is None else dead_letter_exchange,
                                            "x-dead-letter-routing-key" : self.dead_letter_queue if dead_letter_routing_key is None else dead_letter_routing_key,
                                            })
            # self.consume_channel.queue_bind(exchange='amq.direct', queue='task_queue')
    
    @abstractmethod
    def produce_job(self, job_description):
        pass
    
    @abstractmethod
    def do_job(self, job_description):
        '''
            Don't use try catch in do_job function. It will causes problems in callback error system
            When something goes wrong, let raise exception.
            You can return result via set attribute for job_description. 
            You can set anything that can be converted to JSON format
            Example:
            >>> result = {"sum": 3, "product": 2}
            >>> setattr(job_description, self.INDICATOR['current_task_result'], result)
        '''
        pass

    @abstractmethod
    def on_receive_job_handler(self, ch, method, properties, body):
        '''
            On message handler, don't processing job here. Just parse body or header, or ack, nack, etc ....
            Job will be processed in do_job method
        '''
        pass
    
    @abstractmethod
    def run(self):
        '''
            Main method to start worker
        '''
        pass

class RootWorker(BaseWorker):
    def __init__(self, production_key, produce_queue_name, **configs):
        worker_name = "ROOT-WORKER"
        super().__init__(production_key, worker_name, consume_queue_name=None, produce_queue_name=produce_queue_name, **configs)
        # self.consume_channel.connection.close()
        # self.consume_channel.close()

    def _declare_queues(self):
        self._declare_queue(self.produce_queue)

    def produce_job(self, job_description):
        # Root Worker is special, run method is useless so call do_job here. Only Root Worker should call do_job in produce_job method.
        job_description.add_attribute("_timelogs_", [{"service": self.worker_name, "recv_time": time.time()}])
        self.do_job(job_description)
        produce_channel = self.get_produce_instance()
        produce_channel.basic_publish(
                exchange='',
                routing_key=self.produce_queue,
                body=job_description.to_json(),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
            ))
        produce_channel.close()
        produce_channel.connection.close()

    def do_job(self, job_description):
        '''
            You should do only simple procedures when new job added to pipeline here.
            You absolutely shouldn't process job here. Just do something simple.
            You can ignore this method. It isn't necessary
        '''
        return
    
    def on_receive_job_handler(self, ch, method, properties, body):
        '''
            Do nothing in root worker, because there's no consumption.
        '''
        return

    def run(self):
        '''
            RootWorker do not consume any queue.
        '''
        raise Warning("Root Worker do not consume anyqueue. So run method is empty. You don't need to call run. Call produce_job to add a job to pipeline.")
        return

class AbstractWorker(BaseWorker):
    def __init__(self, production_key, worker_name, consume_queue_name, produce_queue_name, **configs):
        self.__warning_worker_name(worker_name)
        super().__init__(production_key, worker_name, consume_queue_name=consume_queue_name, produce_queue_name=produce_queue_name, **configs)
        

    @classmethod
    def __warning_worker_name(cls, worker_name):
        if worker_name == "ROOT-WORKER":
            raise Exception("Worker name ROOT-WORKER only available for root worker. If you want to create Root Worker, use RootWorker class")
        if "root" in worker_name.lower():
            raise Warning("Detected `root` in worker_name. If you want to create Root Worker, use RootWorker class")
            
        if worker_name == "LEAF-WORKER":
            raise Exception("Worker name LEAF-WORKER only available for leaf worker. If you want to create Leaf Worker, use LeafWorker class")
        if "leaf" in worker_name.lower():
            raise Warning("Detected `leaf` in worker_name. If you want to create Leaf Worker, use LeafWorker class")
        
        if worker_name == "DEAD-LETTER-WORKER":
            raise Exception("Worker name LEAF-WORKER only available for leaf worker. If you want to create Leaf Worker, use LeafWorker class")
        if "dead-letter" in worker_name.lower():
            raise Warning("Detected `dead-letter` in worker_name. If you want to create Dead Letter Worker, use Dead-Letter class")
    
    def produce_job(self, job_description):
        produce_channel = self.get_produce_instance()
        produce_channel.basic_publish(
            exchange='',
            routing_key=self.produce_queue,
            body=job_description.to_json(),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
        ))
        produce_channel.close()
        produce_channel.connection.close()
    
    def _declare_queues(self):
        self._declare_queue(self.produce_queue)
        self._declare_queue(self.consume_queue)

    def on_receive_job_handler(self, ch, method, properties, body):
        job_description = JobDescription.fromJson(body)
        # record time log for service
        job_description._timelogs_.append({"service": self.worker_name, "recv_time": time.time()})
        job_description.addAttribute(self.INDICATOR['current_task_name'], self.worker_name)
        try:
            self.do_job(job_description)
            job_description.add_attribute(self.INDICATOR['current_task_status'], True)
            self.produce_job(job_description)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except:
            logging.error(traceback.print_exc())
            job_description.addAttribute(self.INDICATOR['current_task_error'], traceback.format_exc())
            job_description.add_attribute(self.INDICATOR['current_task_status'], False)
            self.sendMessageToDeadLetter(job_description)
            ch.basic_reject(delivery_tag = method.delivery_tag, requeue=False) # Requeue false will send message to specified x-dead-letter-routing-key
    
    def run(self):
        self.consume_channel.basic_consume(self.consume_queue, on_message_callback=self.on_receive_job_handler, auto_ack=False)
        self.consume_channel.start_consuming()

class DelayRequeueWorker(BaseWorker):
    def __init__(self, production_key, worker_name, consume_queue_name, produce_queue_name, delay_requeue_time=1000, **configs):
        self.__warning_worker_name(worker_name)
        super().__init__(production_key, worker_name, consume_queue_name=consume_queue_name, produce_queue_name=produce_queue_name, **configs)
        self.standby_queue = "{}__{}-standby".format(self.production_key, worker_name)
        self._declare_queue(self.standby_queue, message_ttl=delay_requeue_time, dead_letter_routing_key=self.consume_queue)
        self.__delay_requeue_flag = False

    @classmethod
    def __warning_worker_name(cls, worker_name):
        if worker_name == "ROOT-WORKER":
            raise Exception("Worker name ROOT-WORKER only available for root worker. If you want to create Root Worker, use RootWorker class")
        if "root" in worker_name.lower():
            raise Warning("Detected `root` in worker_name. If you want to create Root Worker, use RootWorker class")
            
        if worker_name == "LEAF-WORKER":
            raise Exception("Worker name LEAF-WORKER only available for leaf worker. If you want to create Leaf Worker, use LeafWorker class")
        if "leaf" in worker_name.lower():
            raise Warning("Detected `leaf` in worker_name. If you want to create Leaf Worker, use LeafWorker class")
        
        if worker_name == "DEAD-LETTER-WORKER":
            raise Exception("Worker name LEAF-WORKER only available for leaf worker. If you want to create Leaf Worker, use LeafWorker class")
        if "dead-letter" in worker_name.lower():
            raise Warning("Detected `dead-letter` in worker_name. If you want to create Dead Letter Worker, use Dead-Letter class")
    
    def __get_requeue_indicator(self):
        return f"__{self.standby_queue}__"

    def delay_requeue_job(self, job):
        job.add_attribute(self.__get_requeue_indicator(), True)

    def __send_job_to_standby_queue(self, job_description):
        job_description.del_attribute(self.__get_requeue_indicator())
        produce_channel = self.get_produce_instance()
        produce_channel.basic_publish(
            exchange='',
            routing_key=self.standby_queue,
            body=job_description.to_json(),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
        ))
        produce_channel.close()
        produce_channel.connection.close()

    def produce_job(self, job_description):
        produce_channel = self.get_produce_instance()
        produce_channel.basic_publish(
            exchange='',
            routing_key=self.produce_queue,
            body=job_description.to_json(),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
        ))
        produce_channel.close()
        produce_channel.connection.close()
    
    def _declare_queues(self):
        self._declare_queue(self.produce_queue)
        self._declare_queue(self.consume_queue)

    def on_receive_job_handler(self, ch, method, properties, body):
        job_description = JobDescription.fromJson(body)
        # record time log for service
        # Prevent add multiple time log when loop multi times.
        if len(job_description._timelogs_) > 0:
            last_service = job_description._timelogs_[-1]
            if last_service["service"] != self.worker_name:
                job_description._timelogs_.append({"service": self.worker_name, "recv_time": time.time()})
        job_description.addAttribute(self.INDICATOR['current_task_name'], self.worker_name)
        try:
            self.do_job(job_description)
            job_description.add_attribute(self.INDICATOR['current_task_status'], True)
            if self.__get_requeue_indicator() in job_description:
                self.__send_job_to_standby_queue(job_description)
            else:
                self.produce_job(job_description)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except:
            logging.error(traceback.print_exc())
            job_description.addAttribute(self.INDICATOR['current_task_error'], traceback.format_exc())
            job_description.add_attribute(self.INDICATOR['current_task_status'], False)
            self.sendMessageToDeadLetter(job_description)
            ch.basic_reject(delivery_tag = method.delivery_tag, requeue=False) # Requeue false will send message to specified x-dead-letter-routing-key
    
    def run(self):
        self.consume_channel.basic_consume(self.consume_queue, on_message_callback=self.on_receive_job_handler, auto_ack=False)
        self.consume_channel.start_consuming()

class LeafWorker(BaseWorker):
    def __init__(self, production_key, **configs):
        worker_name = 'LEAF-WORKER'
        super().__init__(production_key, worker_name, **configs)

    def produce_job(self, job_description):
        '''
            Leaf Worker is in the end of pipeline. It absolutely shouldn't produce anything.
        '''
        return
    
    def _declare_queues(self):
        self._declare_queue(self.consume_queue)

    def on_receive_job_handler(self, ch, method, properties, body):
        job_description = JobDescription.fromJson(body)
        # record time log for service
        job_description._timelogs_.append({"service": self.worker_name, "recv_time": time.time()})
        job_description.addAttribute(self.INDICATOR['current_task_name'], self.worker_name)
        try:
            self.do_job(job_description)
            job_description.add_attribute(self.INDICATOR['current_task_status'], True)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except:
            logging.error(traceback.print_exc())
            job_description.addAttribute(self.INDICATOR['current_task_error'], traceback.format_exc())
            job_description.add_attribute(self.INDICATOR['current_task_status'], False)
            self.sendMessageToDeadLetter(job_description)
            ch.basic_reject(delivery_tag = method.delivery_tag, requeue=False) # Requeue false will send message to specified x-dead-letter-routing-key
    
    def run(self):
        self.consume_channel.basic_consume(self.consume_queue, on_message_callback=self.on_receive_job_handler, auto_ack=False)
        self.consume_channel.start_consuming()

class DeadLetterWorker(BaseWorker):
    def __init__(self, production_key, **configs):
        worker_name = 'DEAD-LETTER-WORKER'
        super().__init__(production_key, worker_name, **configs)

    def produce_job(self, job_description):
        '''
            DeadLetter Worker absolutely shouldn't produce anything.
        '''
        return
    
    def _declare_queues(self):
        # Dead Letter Queue is already declared by default.
        return

    def on_receive_job_handler(self, ch, method, properties, body):
        job_description = JobDescription.fromJson(body)
        # Comment below line because DO NOT add worker_name of Dead Letter Worker to jobDescription, because we want to known where is exception thrown
        # job_description.addAttribute(self.INDICATOR['current_task_name'], self.worker_name)
        try:
            # Only process message has flag False
            if job_description[self.INDICATOR['current_task_status']] == True:
                self.do_job(job_description)
            job_description.add_attribute(self.INDICATOR['current_task_status'], True)
        except:
            job_description.addAttribute(self.INDICATOR['current_task_error'], traceback.format_exc())
            job_description.add_attribute(self.INDICATOR['current_task_status'], False)
        
        # Dead Letter Worker always ack received message
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def run(self):
        self.consume_channel.basic_consume(self.consume_queue, on_message_callback=self.on_receive_job_handler, auto_ack=False)
        self.consume_channel.start_consuming()