package com.nghiatd16.Test;

import com.nghiatd16.communication.clientpool.ClusterConnectionProvider;
import com.nghiatd16.communication.common.JobDescription;
import com.nghiatd16.communication.workers.LeafWorker;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.IOException;

class Worker2 extends LeafWorker {


    public Worker2(String productionKey, ClusterConnectionProvider connectionProvider) throws IOException {
        super(productionKey, connectionProvider);
    }

    public Worker2(String productionKey, GenericObjectPoolConfig config, ClusterConnectionProvider connectionProvider) throws IOException {
        super(productionKey, config, connectionProvider);
    }

    @Override
    public void doJob(JobDescription jobDescription) {
        int num_c = (int) jobDescription.get("num_c");
        if (num_c % 2 == 0){
            System.out.println("Ket qua " + num_c + " la 1 so chan");
        }
        else {
            System.out.println("Ket qua " + num_c + " la 1 so le");
        }

    }
}
public class ShowWorker {
    public static void main(String[] args) throws IOException {
        String productionKey = "test-prod";
        ClusterConnectionProvider connectionProvider = new ClusterConnectionProvider("localhost", "5672", "guest", "guest");
        new Worker2(productionKey, connectionProvider).run();
    }
}
