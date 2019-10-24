from worker_communication import RootWorker
from worker_communication import JobDescription

production_key = "TestWorkerCommunication"

class RootService(RootWorker):
    def __init__(self, production_key, produce_queue_name, **configs):
        super().__init__(production_key, produce_queue_name, **configs)
    
    def do_job(self, job_description):
        print("Start Pipeline messages", job_description.toJson())
    
if __name__ == "__main__":
    ls = RootService(production_key, "task_1")
    for i in range(10):
        job_desc = JobDescription(info="Test info")
        ls.produce_job(job_desc)