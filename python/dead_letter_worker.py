from worker_communication import DeadLetterWorker

production_key = "TestWorkerCommunication"

class DeadLetterService(DeadLetterWorker):
    def __init__(self, production_key=production_key, **configs):
        super().__init__(production_key, **configs)
    
    def do_job(self, job_description):
        print("Received dead messages", job_description.toJson())

if __name__ == "__main__":
    dls = DeadLetterService()
    dls.run()