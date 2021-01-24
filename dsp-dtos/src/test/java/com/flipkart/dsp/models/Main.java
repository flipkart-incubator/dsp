package com.flipkart.dsp.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.overrides.DefaultDataframeOverride;
import com.flipkart.dsp.models.overrides.PartitionDataframeOverride;
import com.flipkart.dsp.models.overrides.RunIdDataframeOverride;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*Test class to verify serialisation and de-serialisation. */
public class Main {
    public static void main(String[] args) {

        RunIdDataframeOverride runIdDataframeOverride = new RunIdDataframeOverride(2000L);
        PartitionDataframeOverride partitionDataframeOverride = new PartitionDataframeOverride();
        partitionDataframeOverride.put("s1#v1","location1");
        DefaultDataframeOverride defaultDataframeOverride = new DefaultDataframeOverride(true);
        Map<String, DataframeOverride> dataframeOverrideMap = new HashMap<>();
        dataframeOverrideMap.put("df1", defaultDataframeOverride);
        dataframeOverrideMap.put("df2", runIdDataframeOverride);
        dataframeOverrideMap.put("df3", partitionDataframeOverride);

        List<String> partitionList = new ArrayList();
        partitionList.add("s13v1");

        partitionDataframeOverride.put("s1#v1", "modelLocation1");

        RequestOverride requestOverride =
                new RequestOverride(dataframeOverrideMap,null);

        WorkflowGroupExecuteRequest workflowGroupExecuteRequest =
                new WorkflowGroupExecuteRequest(1L,false,"callbackUrl://", new HashMap<String, Long>() {
                    {
                        put("t1",1L);
                        put("t2",2L);
                    }
                }, new HashMap<String , RequestOverride>() {
                    {
                        put("w1", requestOverride);
                    }
                }, null);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String serialisedString = objectMapper.writeValueAsString(workflowGroupExecuteRequest);
            System.out.println(serialisedString);
            WorkflowGroupExecuteRequest workflowGroupExecuteRequest1 = objectMapper.readValue(serialisedString, WorkflowGroupExecuteRequest.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
