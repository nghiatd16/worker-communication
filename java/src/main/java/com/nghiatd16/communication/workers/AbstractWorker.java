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

public abstract class AbstractWorker extends BaseWorker {

    public AbstractWorker(String productionKey, String workerName, String consumeQueueName, String produceQueueName, ClusterConnectionProvider connectionProvider) throws IOException {
        super(productionKey, workerName, consumeQueueName, produceQueueName, connectionProvider);
    }

    public AbstractWorker(String productionKey, String workerName, String consumeQueueName, String produceQueueName, GenericObjectPoolConfig config, ClusterConnectionProvider connectionProvider) throws IOException {
        super(productionKey, workerName, consumeQueueName, produceQueueName, config, connectionProvider);
    }

    @Override
    public void produceJob(JobDescription jobDescription) {
        sendMessageToQueue(produceQueueName, jobDescription);
    }

    @Override
    protected void declareQueues() {
        declareQueue(this.consumeQueueName);
        declareQueue(this.produceQueueName);
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
            if (produceQueueName != null) {
                produceJob(jd);
            }
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
