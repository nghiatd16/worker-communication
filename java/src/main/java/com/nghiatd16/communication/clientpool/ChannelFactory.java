package com.nghiatd16.communication.clientpool;

import com.nghiatd16.communication.clientpool.exceptions.ChannelException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;


public class ChannelFactory implements PooledObjectFactory<Channel> {
    private Connection connection;

    public ChannelFactory() {
        this(null);
    }

    public ChannelFactory(ClusterConnectionProvider connProvider) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setCredentialsProvider(connProvider);
            connection = factory.newConnection(connProvider.getClusterAddresses());
        } catch (Exception e) {
            throw new ChannelException("Connection failed", e);
        }
    }

    public PooledObject<Channel> makeObject() throws Exception {
        return new DefaultPooledObject<Channel>(connection.createChannel());
    }

    public void destroyObject(PooledObject<Channel> pooledObject) throws Exception {
        final Channel channel = pooledObject.getObject();
        if (channel.isOpen()) {
            try {
                channel.close();
            } catch (Exception e) {
            }
        }
    }

    public boolean validateObject(PooledObject<Channel> pooledObject) {
        final Channel channel = pooledObject.getObject();
        return channel.isOpen();
    }

    public void activateObject(PooledObject<Channel> pooledObject) throws Exception {

    }

    public void passivateObject(PooledObject<Channel> pooledObject) throws Exception {

    }
}
