package com.nghiatd16.Test;

import com.nghiatd16.communication.clientpool.ClusterConnectionProvider;
import com.nghiatd16.communication.common.JobDescription;
import com.nghiatd16.communication.workers.AbstractWorker;
import com.nghiatd16.communication.workers.BaseWorker;

import java.io.IOException;

class Worker1 extends AbstractWorker{

    public Worker1(String productionKey, String workerName, String consumeQueueName, String produceQueueName, ClusterConnectionProvider connectionProvider) throws IOException {
        super(productionKey, workerName, consumeQueueName, produceQueueName, connectionProvider);
    }

    @Override
    public void doJob(JobDescription jobDescription) throws Exception {
        int num_a = (int) jobDescription.get("num_a");
        int num_b = (int) jobDescription.get("num_b");
        int total = num_a + num_b;
        jobDescription.addAttribute("num_c", total);
        System.out.println(jobDescription);
    }
}
public class SumWorker {
    public static void main(String[] args) throws IOException {
        String productionKey = "test-prod";
        ClusterConnectionProvider connectionProvider = new ClusterConnectionProvider("localhost", "5672", "guest", "guest");
        Worker1 w = new Worker1(productionKey, "sum-worker", "sum-worker", BaseWorker.LEAF_WORKER_NAME, connectionProvider);
        w.run();
    }
}
