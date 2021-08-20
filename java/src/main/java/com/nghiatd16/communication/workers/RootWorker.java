package com.nghiatd16.communication.workers;

import com.nghiatd16.communication.common.JobDescription;
import com.nghiatd16.communication.clientpool.ClusterConnectionProvider;
import com.nghiatd16.communication.helpers.TimeHelper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;

public class RootWorker extends BaseWorker {

    public RootWorker(String productionKey, String produceQueueName, ClusterConnectionProvider connectionProvider) throws IOException {
        super(productionKey, ROOT_WORKER_NAME, null, produceQueueName, connectionProvider);
    }

    public RootWorker(String productionKey, String produceQueueName, GenericObjectPoolConfig config, ClusterConnectionProvider connectionProvider) throws IOException {
        super(productionKey, ROOT_WORKER_NAME, null, produceQueueName, config, connectionProvider);
    }

    @Override
    protected void declareQueues() {
        declareQueue(this.produceQueueName);
    }

    @Override
    public void produceJob(JobDescription jobDescription) {
//        Root Worker is special, run method is useless so call do_job here. Only Root Worker should call do_job in produce_job method.
        JSONObject timelogjson = new JSONObject();
        timelogjson.put("service", this.workerName);
        timelogjson.put("recv_time", TimeHelper.Instance.getLocalNowTimestamp());
        jobDescription.addAttribute("_timelogs_", new JSONArray(Arrays.asList(timelogjson)));
//        Call doJob
        doJob(jobDescription);
        sendMessageToQueue(produceQueueName, jobDescription);
    }

    @Override
    public void doJob(JobDescription jobDescription) {

    }

    @Override
    public void onReceiveJobHandler(Channel channel, String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {

    }

    @Override
    public void run() {

    }
}

