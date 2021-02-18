package com.nghiatd16.communication;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;
import org.apache.commons.lang3.tuple.Pair;

public class JobDescription {
    private JSONObject data;

    public <V> JobDescription(Pair<String,V>... kwargs){
        this.data = new JSONObject();
        for(Pair<String,V> ele : kwargs){
            this.data.put(ele.getKey(), ele.getValue());
        }
    }

    public JobDescription(String jsonString) throws JSONException{
        this.data = new JSONObject(jsonString);
    }

    public <V> void addAttribute(String name, V value){
        if (this.data.has(name)){
            this.data.remove(name);
        }
        this.data.put(name, value);
    }

    public void delAttribute(String name){
        if (this.data.has(name)){
            this.data.remove(name);
        }
    }

    public Object get(String name){
        return this.data.get(name);
    }

    public boolean contains(String name){
        return this.data.has(name);
    }

    public String toJson() {
        return this.data.toString();
    }

    public static JobDescription fromJson(String jsonString) throws JSONException{
        return new JobDescription(jsonString);
    }

    public String toString() {
        return this.data.toString();
    }
}