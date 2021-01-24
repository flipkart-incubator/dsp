package com.flipkart.dsp.jobs;

import azkaban.utils.Props;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.module.AzkabanModule;
import com.flipkart.dsp.utils.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.Loader;
import org.apache.log4j.helpers.OptionConverter;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.util.*;

import static com.flipkart.dsp.utils.Constants.AZKABAN_HOST_IP;
import static com.flipkart.dsp.utils.Constants.CONFIG_SVC_BUCKETS_KEY;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.log4j.LogManager.DEFAULT_CONFIGURATION_FILE;

/**
 *
 */
@Slf4j
public class JobDriverV2 {
    private static final String DEBUG_MODE = "DEBUG_MODE";
    private Props props;
    private static Map<String, List<String>> map = new HashMap<>();


    public JobDriverV2(String name, Props props) {
        populateMap();
        this.props = props;
        Logger rootLogger = Logger.getRootLogger();
        Logger mylogger = LogManager.getLogger("MYLOGGER");
        rootLogger.addAppender(mylogger.getAppender("MYLOGGER"));
        URL url = Loader.getResource(DEFAULT_CONFIGURATION_FILE);
        OptionConverter.selectAndConfigure(url, null,
                LogManager.getLoggerRepository());
    }

    private void populateMap() {
        map.put(AzkabanModule.class.getCanonicalName(), new ArrayList<>(Arrays.asList(
                AzkabanImageDetailsNode.class.getSimpleName(),
                AzkabanTerminalNode.class.getSimpleName(),
                AzkabanWorkflowNode.class.getSimpleName(),
                AzkabanOutputIngestionNode.class.getSimpleName(),
                AzkabanImageDetailsNode.class.getSimpleName(),
                AzkabanExternalClientHealthCheckNode.class.getSimpleName())));
    }

    public static String getPropertyAsString(Properties prop) throws IOException {
        StringWriter writer = new StringWriter();
        try {
            prop.store(writer, "");
        } catch (IOException e) {
            log.error("Error occurred while converting property object to String");
            throw e;
        }
        return writer.getBuffer().toString();
    }

    public Props getJobGeneratedProperties() {
        return props;
    }

    public void run() throws IOException, ClassNotFoundException, AzkabanException, IllegalAccessException, InstantiationException {
        Properties properties = props.toProperties();

        checkNotNull(properties.getProperty(Constants.APPLICATION_CLASS));

        Class clazz = Class.forName(properties.getProperty(Constants.APPLICATION_CLASS));

        String inNodes = properties.getProperty(Constants.AZKABAN_IN_NODES);
        String azkabanCurrentNodeID = properties.getProperty(Constants.AZKABAN_JOB_ID);

        boolean debugMode = Boolean.valueOf(properties.getProperty(DEBUG_MODE));

        if (debugMode) {
            //This gives time for the debugger to enable Remote Debug
            try {
                Thread.sleep(15 * 1000);
            } catch (InterruptedException e) {
                log.error(e.getMessage());
            }
        }

        System.setProperty(CONFIG_SVC_BUCKETS_KEY, properties.getProperty(CONFIG_SVC_BUCKETS_KEY));


        log.info("getting actual dynamic args with inNodes : {}", inNodes);


        String[] args = {getPropertyAsString(properties)};
        log.info("Dynamic Args for the Node {} : {}", inNodes, args);

        Class abstractModuleClass = getAbstractModuleClass(clazz);

        NodeMetaData jobMap = SessionfulApplicationExecutor.execute(clazz, abstractModuleClass, args, properties.getProperty(AZKABAN_HOST_IP));
        if (!clazz.getSimpleName().equals(AzkabanNotifierNode.class.getSimpleName())
                && !clazz.getSimpleName().equals(AzkabanImageDetailsNode.class.getSimpleName())
                && !clazz.getSimpleName().equals(AzkabanExternalClientHealthCheckNode.class.getSimpleName())) {
            checkNotNull(jobMap, "Error while running " + clazz.getSimpleName());
        }

        log.info("updating props with key: {}, value: {}", azkabanCurrentNodeID + Constants.APPLICATION_CLASS_DYNAMIC_ARGS, jobMap);
        props.put(Constants.APPLICATION_CLASS_DYNAMIC_ARGS, JsonUtils.DEFAULT.toJson(jobMap));
    }

    private Class getAbstractModuleClass(Class clazz) throws ClassNotFoundException {
        Class requiredClass = null;
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            if (entry.getValue().contains(clazz.getSimpleName())) {
                requiredClass = Class.forName(entry.getKey());
            }
        }
        if (requiredClass == null) {
            throw new ClassNotFoundException("Unable to find Module class for class " + clazz.getSimpleName());
        }
        return requiredClass;
    }
}
