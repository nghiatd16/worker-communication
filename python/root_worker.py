from worker_communication import RootWorker
from worker_communication import JobDescription
from worker_communication import ClusterConnectionParams

production_key = "TestWorkerCommunication"

all_cluster_node = [
    ClusterConnectionParams()
]

class RootService(RootWorker):
    def __init__(self, production_key, produce_queue_name, connection_params_list):
        super().__init__(production_key, produce_queue_name, connection_params_list)
    
    def do_job(self, job_description):
        print("Start Pipeline messages", job_description.toJson())
    
if __name__ == "__main__":
    ls = RootService(production_key, "task_1", all_cluster_node)
    for i in range(10):
        job_desc = JobDescription(info="Test info")
        ls.produce_job(job_desc)