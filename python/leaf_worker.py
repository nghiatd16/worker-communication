from worker_communication import LeafWorker

production_key = "TestWorkerCommunication"

class LeafService(LeafWorker):
    def __init__(self, production_key, consume_queue_name=None, **configs):
        super().__init__(production_key, consume_queue_name=consume_queue_name, **configs)

    
    def do_job(self, job_description):
        print("End of Pipeline messages", job_description.toJson())
    
if __name__ == "__main__":
    ls = LeafService(production_key)
    ls.run()