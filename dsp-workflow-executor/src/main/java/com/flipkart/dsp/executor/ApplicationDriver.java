package com.flipkart.dsp.executor;

import com.flipkart.dsp.executor.application.AbstractApplication;
import com.flipkart.dsp.executor.exception.ApplicationException;
import com.flipkart.dsp.executor.executor.ApplicationExecutor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApplicationDriver {
    /**
     * @param args args[0] -> className ie: MetricComputeDriver
     *             args[1] -> bucket name
     *             args[2] -> ConfigPayload
     */
    public static void main(String args[]) throws ApplicationException {
        Class clazz;
        String className = AbstractApplication.class.getPackage().getName() + "." + args[0];
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new ApplicationException("Class not found : " + className, e);
        }

        String configBucket = args[1];
        String serializedConfigPayload = args[2];
        String frameWorkId = args[3];
        String slaveId = args[4];
        String hostId = args[5];
        String executorId = args[6];
        String cpus = args[7];
        String mem = args[8];
        String role = args[9];
        String attempt = args[10];
        String previousAttemptContainerId = args[11];
        String[] argList = {serializedConfigPayload, frameWorkId, slaveId, hostId, executorId, cpus, mem, role,
                attempt, previousAttemptContainerId};
        log.info("executing : {} with serializedPayload: {}", clazz, serializedConfigPayload);
        ApplicationExecutor applicationExecutor = new ApplicationExecutor(configBucket, hostId);
        try {
            applicationExecutor.execute(clazz, argList);
        } catch (ApplicationException | RuntimeException e) {
            log.error("Application {} failed to run because of following reason: ", className, e);
            System.exit(1);
        }
        System.exit(0);
    }
}
