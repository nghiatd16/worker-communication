from worker_communication import AbstractWorker
from random import randint
import time
from worker_communication import ClusterConnectionParams

production_key = "TestWorkerCommunication"

all_cluster_node = [
    ClusterConnectionParams()
]

class Task2Service(AbstractWorker):
    def __init__(self, production_key, worker_name, consume_queue_name, produce_queue_name, connection_params_list):
        super().__init__(production_key, worker_name, consume_queue_name, produce_queue_name, connection_params_list)
    
    def do_job(self, job_description):
        print("Task 2 received messages", job_description.toJson())
        rand_number = randint(1, 10)
        job_description.addAttribute("task_2", rand_number)
        task_1 = job_description.task_1
        if (task_1 % 2) == (rand_number % 2):
            raise Exception("Test Exception Task 2")
        time.sleep(rand_number//2)

if __name__ == "__main__":
    ls = Task2Service(production_key, "task_2", "task_2", AbstractWorker.LEAF_WORKER_NAME, all_cluster_node)
    ls.run()