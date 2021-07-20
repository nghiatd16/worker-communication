package com.nghiatd16;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.Arrays;

import com.nghiatd16.communication.JobDescription;
/**
 * Hello world!
 */
public final class App {

	private App() {
    }

    /**
     * Says hello to the world.
     * 
     * @param args The arguments of the program.
     */

    public static void main(String[] args) throws JSONException{
        String jsonString = "{\"name\":\"John\", \"age\":31, \"city\":\"New York\"}";
        JobDescription jd = new JobDescription(jsonString);
        jd.addAttribute("hobby", new JSONArray(Arrays.asList("eat", "sleep")));
        JSONArray hobby = (JSONArray) jd.get("hobby");
        hobby.put("race");
        System.out.println(jd);
        
    }
}
