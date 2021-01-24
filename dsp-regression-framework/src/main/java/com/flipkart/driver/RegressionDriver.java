package com.flipkart.driver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dto.DSPTestEnvVariables;
import com.flipkart.dto.TestEnvVariables;
import com.flipkart.module.RegressionExecutor;
import com.flipkart.team.dsp.DSPTeam;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RegressionDriver {

    public static void main(String args[]) {
        try {
        RegressionExecutor regressionExecutor = new RegressionExecutor();
        ObjectMapper objectMapper = new ObjectMapper();
        TestEnvVariables testEnvVariables = objectMapper.readValue(args[0], TestEnvVariables.class);
        Class clazz;
            if (testEnvVariables instanceof DSPTestEnvVariables) {
                clazz = Class.forName(DSPTeam.class.getName());
                regressionExecutor.execute(clazz, testEnvVariables);
                System.exit(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
            // In case of exception we need to end the jar
            System.exit(1);
        }
    }
}
