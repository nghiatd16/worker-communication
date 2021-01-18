from worker_communication import LeafWorker
from worker_communication import ClusterConnectionParams

production_key = "TestWorkerCommunication"

all_cluster_node = [
    ClusterConnectionParams()
]
class LeafService(LeafWorker):
    def __init__(self, production_key, connection_params_list):
        super().__init__(production_key, connection_params_list)

    
    def do_job(self, job_description):
        print("End of Pipeline messages", job_description.toJson())
    
if __name__ == "__main__":
    ls = LeafService(production_key, all_cluster_node)
    ls.run()