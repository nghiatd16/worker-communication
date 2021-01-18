from worker_communication import DeadLetterWorker
from worker_communication import ClusterConnectionParams

production_key = "TestWorkerCommunication"

all_cluster_node = [
    ClusterConnectionParams()
]
class DeadLetterService(DeadLetterWorker):
    def __init__(self, production_key, connection_params_list):
        super().__init__(production_key, connection_params_list)
    
    def do_job(self, job_description):
        print("Received dead messages", job_description.toJson())

if __name__ == "__main__":
    dls = DeadLetterService(production_key, all_cluster_node)
    dls.run()