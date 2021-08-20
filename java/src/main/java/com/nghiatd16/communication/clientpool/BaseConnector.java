package com.nghiatd16.communication;

import com.nghiatd16.communication.clientpool.ChannelFactory;
import com.nghiatd16.communication.clientpool.ChannelPool;
import com.nghiatd16.communication.clientpool.ClusterConnectionProvider;
import com.rabbitmq.client.Channel;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.IOException;
import java.util.HashMap;


public class BaseConnector {
    public static final HashMap<String, String> INDICATOR = new HashMap<String, String>() {{
        put("current_task_name", "__indicator__current_task_name");
        put("current_task_status", "__indicator__current_task_status");
        put("current_task_result", "__indicator__current_task_result");
        put("current_task_error", "__indicator__current_task_error");
    }};

    private ChannelPool pool;
    public BaseConnector(ClusterConnectionProvider connectionProvider) {
        this.pool = new ChannelPool(ChannelPool.defaultConfig, new ChannelFactory(connectionProvider));
    }
    public BaseConnector(GenericObjectPoolConfig config, ClusterConnectionProvider connectionProvider){
        this.pool = new ChannelPool(config, new ChannelFactory(connectionProvider));
    }

    public Channel getChannelInstance() throws IOException {
        Channel channel = this.pool.getChannel();
//      Fair dispatch. More detail read Fair dispatch in https://www.rabbitmq.com/tutorials/tutorial-two-python.html
        channel.basicQos(1);
        return channel;
    }

    public Channel getProducerInstance() throws IOException {
        return getChannelInstance();
    }

    public Channel getConsumerInstance() throws IOException {
        return getChannelInstance();
    }

    public void releaseChannel(Channel channel){
        this.pool.returnChannel(channel);
    }
}
