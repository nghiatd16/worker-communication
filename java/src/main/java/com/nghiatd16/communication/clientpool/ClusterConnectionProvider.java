package com.nghiatd16.communication.clientpool;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.impl.CredentialsProvider;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClusterConnectionProvider implements CredentialsProvider {
    private List<Address> clusterAddresses;
    private String user;
    private String pwd;

    public ClusterConnectionProvider(List<String> hosts, List<Integer> ports, String user, String pwd) throws InvalidParameterException {
        if(hosts.size() != ports.size()){
            throw new InvalidParameterException("Hosts list and Ports list must be had same size");
        }
        clusterAddresses = new ArrayList<>();
        for (int i = 0 ; i < hosts.size(); i++){
            clusterAddresses.add(new Address(hosts.get(i), ports.get(i)));
        }
        this.user = user;
        this.pwd = pwd;
    }

    public ClusterConnectionProvider(String hosts, String ports, String user, String pwd) throws InvalidParameterException {
        List<String> listHosts = Arrays.asList(hosts.split(","));
        List<String> listStringPorts = Arrays.asList(ports.split(","));
        List<Integer> listPorts = new ArrayList<>();
        for(String sp : listStringPorts){
            listPorts.add(Integer.valueOf(sp));
        }

        if(listHosts.size() != listPorts.size()){
            throw new InvalidParameterException("Hosts list and Ports list must be had same size");
        }
        clusterAddresses = new ArrayList<>();
        for (int i = 0 ; i < listHosts.size(); i++){
            clusterAddresses.add(new Address(listHosts.get(i), listPorts.get(i)));
        }
        this.user = user;
        this.pwd = pwd;
    }


    @Override
    public String getUsername() {
        return this.user;
    }

    @Override
    public String getPassword() {
        return this.pwd;
    }

    public List<Address> getClusterAddresses() {
        return this.clusterAddresses;
    }
}
