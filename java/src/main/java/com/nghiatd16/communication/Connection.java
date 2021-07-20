package com.nghiatd16.communication;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.CredentialsProvider;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import org.apache.commons.lang3.tuple.Pair;
import java.util.HashMap;
import java.util.Vector;

class ClusterConnectionParams {
    private String host;
    private String port;
    private String user;
    private String pwd;

    public ClusterConnectionParams(String host, String port, String user, String pwd) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.pwd = pwd;
    }

    public String toString() {

        return "[Host: " + this.host + "; Port: " + this.port + "; User: " + this.user + "; PWD: " + this.pwd + "]";
    }

}

class BaseConnector {
    public static final HashMap<String, String> INDICATOR = new HashMap<String, String>() {{
        put("current_task_name", "__indicator__current_task_name");
        put("current_task_status", "__indicator__current_task_status");
        put("current_task_result", "__indicator__current_task_result");
        put("current_task_error", "__indicator__current_task_error");
    }};

    private Vector<ConnectionFactory> all_endpoints;
    public BaseConnector(ClusterConnectionParams[] connection_params_list) {
        this.all_endpoints = new Vector<>();
        ConnectionFactory factory;
        for(ClusterConnectionParams param : connection_params_list){
            factory = new ConnectionFactory();
            factory.setCredentialsProvider(new );
        }
    }
}