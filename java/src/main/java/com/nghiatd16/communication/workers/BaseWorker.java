package com.nghiatd16.communication.workers;

import com.nghiatd16.communication.clientpool.BaseConnector;
import com.nghiatd16.communication.common.JobDescription;
import com.nghiatd16.communication.clientpool.ClusterConnectionProvider;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseWorker extends BaseConnector {
    public static final String ROOT_WORKER_NAME = "ROOT-WORKER";
    public static final String LEAF_WORKER_NAME = "LEAF-WORKER";
    public static final String DEAD_LETTER_WORKER_NAME = "DEAD-LETTER-QUEUE";
    protected String productionKey = null;
    protected String workerName = null;
    protected String consumeQueueName = null;
    protected String produceQueueName = null;

    public BaseWorker(String productionKey, String workerName,
                      String consumeQueueName, String produceQueueName,
                      ClusterConnectionProvider connectionProvider) throws IOException {
        super(connectionProvider);
        initialize(productionKey, workerName, consumeQueueName, produceQueueName);
    }

    public BaseWorker(String productionKey, String workerName,
                      String consumeQueueName, String produceQueueName,
                      GenericObjectPoolConfig config, ClusterConnectionProvider connectionProvider) throws IOException {
        super(config, connectionProvider);
        initialize(productionKey, workerName, consumeQueueName, produceQueueName);
    }

    protected void initialize(String productionKey, String workerName,
                              String consumeQueueName, String produceQueueName) throws IOException {
        this.productionKey = productionKey;
        this.workerName = workerName;

        if (consumeQueueName != null) {
            this.consumeQueueName = this.productionKey + "__" + consumeQueueName;
        } else if (workerName.equals(DEAD_LETTER_WORKER_NAME)) {
            this.consumeQueueName = getDeadWorkerQueueName(productionKey);
        } else if (workerName.equals(LEAF_WORKER_NAME)) {
            this.consumeQueueName = getLeafWorkerQueueName(productionKey);
        }

        if (produceQueueName != null) {
            this.produceQueueName = this.productionKey + "__" + produceQueueName;
        }

//        Define Dead-Letter-Queue
        Channel consumeChannel = getConsumerInstance();
//        Set durable = true. More detail https://www.rabbitmq.com/queues.html
        consumeChannel.queueDeclare(getDeadWorkerQueueName(productionKey), true, false, false, null);
        releaseChannel(consumeChannel);
        declareQueues();

    }

    public static final String getLeafWorkerQueueName(String productionKey) {
        return productionKey + "__" + LEAF_WORKER_NAME;
    }

    public static final String getDeadWorkerQueueName(String productionKey) {
        return productionKey + "__" + DEAD_LETTER_WORKER_NAME;
    }

    protected abstract void declareQueues();

    protected void declareQueue(String queueName, int message_ttl, String deadLetterExchange, String deadLetterRoutingKey) {
        if (queueName != null) {
            Channel consumeChannel = null;
            try {
                consumeChannel = getConsumerInstance();
                Map<String, Object> args = new HashMap<>();
                args.put("x-message-ttl", message_ttl);
                if (deadLetterExchange == null) {
                    args.put("x-dead-letter-exchange", "");
                } else {
                    args.put("x-dead-letter-exchange", deadLetterExchange);
                }
                if (deadLetterRoutingKey == null) {
                    args.put("x-dead-letter-exchange", getDeadWorkerQueueName(productionKey));
                } else {
                    args.put("x-dead-letter-routing-key", deadLetterRoutingKey);
                }

                consumeChannel.queueDeclare(queueName, true, false, false, args);
            } catch (Exception e1) {
                e1.printStackTrace();
            } finally {
                if (consumeChannel != null) {
                    releaseChannel(consumeChannel);
                }
            }
        }
    }

    protected void declareQueue(String queueName) {
        declareQueue(queueName, 3600000, null, null);
    }

    public void sendMessageToDeadLetter(JobDescription jobDescription) {
        sendMessageToQueue(getDeadWorkerQueueName(productionKey), jobDescription);
    }

    public void sendMessageToQueue(String queueName, JobDescription jobDescription) {
        Channel produceChannel = null;
        try {
            produceChannel = getProducerInstance();
//            Make message persistent
            AMQP.BasicProperties properties = new AMQP.BasicProperties().builder().contentType("text/plain").deliveryMode(2).build();
            produceChannel.basicPublish("", queueName, properties, jobDescription.toString().getBytes());
        } catch (Exception e1) {
            e1.printStackTrace();
        } finally {
            if (produceChannel != null) {
                releaseChannel(produceChannel);
            }
        }
    }

    abstract public void produceJob(JobDescription jobDescription);

    abstract public void doJob(JobDescription jobDescription);

    abstract public void onReceiveJobHandler(Channel channel,
                                             String consumerTag,
                                             Envelope envelope,
                                             AMQP.BasicProperties properties,
                                             byte[] body) throws IOException;

    public void run() {
        Channel consumeChannel = null;
        try {
            consumeChannel = getConsumerInstance();
            Channel finalConsumeChannel = consumeChannel;
            consumeChannel.basicConsume(consumeQueueName, false,
                new DefaultConsumer(finalConsumeChannel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) throws IOException {
                        onReceiveJobHandler(finalConsumeChannel, consumerTag, envelope, properties, body);
                    }
                });
            System.err.println("Worker " + workerName + " started");
        } catch (Exception e1) {
            e1.printStackTrace();
            System.err.println("Worker " + workerName + " start failed");
        }
    }

    ;
}
