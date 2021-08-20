package com.nghiatd16.communication.workers;

import com.nghiatd16.communication.common.JobDescription;
import com.nghiatd16.communication.clientpool.ClusterConnectionProvider;
import com.nghiatd16.communication.helpers.TimeHelper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public abstract class LeafWorker extends BaseWorker {

    public LeafWorker(String productionKey, ClusterConnectionProvider connectionProvider) throws IOException {
        super(productionKey, LEAF_WORKER_NAME, null, null, connectionProvider);
    }

    public LeafWorker(String productionKey, GenericObjectPoolConfig config, ClusterConnectionProvider connectionProvider) throws IOException {
        super(productionKey, LEAF_WORKER_NAME, null, null, config, connectionProvider);
    }

    @Override
    protected void declareQueues() {
        declareQueue(consumeQueueName);
    }

    @Override
    public void produceJob(JobDescription jobDescription) {
//        Leaf Worker is in the end of pipeline. It absolutely shouldn't produce anything.
    }

    @Override
    public void onReceiveJobHandler(Channel channel, String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        JobDescription jd = JobDescription.fromJson(new String(body, StandardCharsets.UTF_8));
        JSONObject timelogjson = new JSONObject();
        timelogjson.put("service", this.workerName);
        timelogjson.put("recv_time", TimeHelper.Instance.getLocalNowTimestamp());
        JSONArray timelogarray = (JSONArray) jd.get("_timelogs_");
        timelogarray.put(timelogjson);
        jd.addAttribute(INDICATOR.get("current_task_name"), workerName);
        try {
            doJob(jd);
            jd.addAttribute(INDICATOR.get("current_task_status"), true);
            channel.basicAck(envelope.getDeliveryTag(), false);
        } catch (Exception e1) {
            String traceback = ExceptionUtils.getStackTrace(e1);
            System.err.println(traceback);
            jd.addAttribute(INDICATOR.get("current_task_error"), traceback);
            jd.addAttribute(INDICATOR.get("current_task_status"), false);
            sendMessageToDeadLetter(jd);
//            Requeue false will send message to specified x-dead-letter-routing-key
            channel.basicReject(envelope.getDeliveryTag(), false);
        }
    }
}
