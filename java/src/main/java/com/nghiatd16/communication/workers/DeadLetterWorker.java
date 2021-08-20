package com.nghiatd16.communication.workers;

import com.nghiatd16.communication.common.JobDescription;
import com.nghiatd16.communication.clientpool.ClusterConnectionProvider;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public abstract class DeadLetterWorker extends BaseWorker {

    public DeadLetterWorker(String productionKey, String workerName, String consumeQueueName, String produceQueueName, ClusterConnectionProvider connectionProvider) throws IOException {
        super(productionKey, workerName, consumeQueueName, produceQueueName, connectionProvider);
    }

    public DeadLetterWorker(String productionKey, String workerName, String consumeQueueName, String produceQueueName, GenericObjectPoolConfig config, ClusterConnectionProvider connectionProvider) throws IOException {
        super(productionKey, workerName, consumeQueueName, produceQueueName, config, connectionProvider);
    }

    @Override
    protected void declareQueues() {
//        Dead Letter Queue is already declared by default
    }

    @Override
    public void produceJob(JobDescription jobDescription) {
//        DeadLetter Worker absolutely shouldn't produce anything
    }

    @Override
    public void onReceiveJobHandler(Channel channel, String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        JobDescription jd = JobDescription.fromJson(new String(body, StandardCharsets.UTF_8));
        try {
            if ((Boolean) jd.get(INDICATOR.get("current_task_error")) == false) {
                doJob(jd);
            }
            jd.addAttribute(INDICATOR.get("current_task_status"), true);
        } catch (Exception e1) {
            String traceback = ExceptionUtils.getStackTrace(e1);
            System.err.println(traceback);
            jd.addAttribute(INDICATOR.get("current_task_error"), traceback);
            jd.addAttribute(INDICATOR.get("current_task_status"), false);
//            Requeue false will send message to specified x-dead-letter-routing-key
        } finally {
//            Dead Letter Worker always ack received message
            channel.basicAck(envelope.getDeliveryTag(), false);
        }
    }
}
