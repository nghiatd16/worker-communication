from worker_communication import AbstractWorker
from random import randint
import time
from worker_communication import ClusterConnectionParams

production_key = "TestWorkerCommunication"

all_cluster_node = [
    ClusterConnectionParams()
]

class Task1Service(AbstractWorker):
    def __init__(self, production_key, worker_name, consume_queue_name, produce_queue_name, connection_params_list):
        super().__init__(production_key, worker_name, consume_queue_name, produce_queue_name, connection_params_list)
    
    def do_job(self, job_description):
        print("Task 1 received messages", job_description.toJson())
        rand_number = randint(1, 10)
        job_description.addAttribute("task_1", rand_number)
        if rand_number == 5:
            raise Exception("Test Exception Task 1")
        time.sleep(rand_number//2)
    
if __name__ == "__main__":
    ls = Task1Service(production_key, "task_1", "task_1", "task_2", all_cluster_node)
    ls.run()