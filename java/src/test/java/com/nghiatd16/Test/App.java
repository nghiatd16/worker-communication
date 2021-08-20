package com.nghiatd16.Test;
import com.nghiatd16.communication.clientpool.ClusterConnectionProvider;
import com.nghiatd16.communication.workers.RootWorker;
import org.json.JSONException;

import java.io.IOException;

import com.nghiatd16.communication.common.JobDescription;
/**
 * Hello world!
 */

class HTTPService extends RootWorker{

    public HTTPService(String productionKey, String produceQueueName, ClusterConnectionProvider connectionProvider) throws IOException {
        super(productionKey, produceQueueName, connectionProvider);
    }
}

public final class App {


    public static void main(String[] args) throws JSONException, IOException {
        String productionKey = "test-prod";
        ClusterConnectionProvider connectionProvider = new ClusterConnectionProvider("localhost,127.0.1.1", "5672,5672", "guest", "guest");
        HTTPService rootService = new HTTPService(productionKey, "sum-worker", connectionProvider);
        JobDescription jd = new JobDescription();
        jd.addAttribute("num_a", 5);
        jd.addAttribute("num_b", 2);

        rootService.produceJob(jd);

        jd.addAttribute("num_a", 1231);
        jd.addAttribute("num_b", 4);

        rootService.produceJob(jd);

    }
}
