from worker_communication import LeafWorker

production_key = "TestWorkerCommunication"

class LeafService(LeafWorker):
    def __init__(self, production_key, **configs):
        super().__init__(production_key, **configs)

    
    def do_job(self, job_description):
        print("End of Pipeline messages", job_description.toJson())
    
if __name__ == "__main__":
    ls = LeafService(production_key)
    ls.run()