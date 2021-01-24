package com.flipkart.dsp.jobs;

import azkaban.jobExecutor.ProcessJob;
import azkaban.utils.Props;
import org.apache.log4j.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.flipkart.dsp.utils.Constants.ENV;

public class AzkabanJobRunner {

  public static final String[] PROPS_CLASSES = new String[]{
      "azkaban.utils.Props",
      "azkaban.common.utils.Props"
  };
  public static final String RUN_METHOD_PARAM = "method.run";
  public static final String DEFAULT_RUN_METHOD = "run";

  public static final String DRIVER_CLASS = "com.flipkart.dsp.jobs.JobDriverV2";

  private static final Layout DEFAULT_LAYOUT = new PatternLayout("%p %m\n");

  private final Logger _logger;
  private final String _jobName;
  private final Object _javaObject;

  public AzkabanJobRunner() throws Exception {
    _logger = createLogger();

    _jobName = System.getenv(ProcessJob.JOB_NAME_ENV);

    Props props = new Props(null, getClass().getResourceAsStream("/fixtures/test_job.properties"));
    Properties properties = props.toProperties();

    _javaObject = getObject(_jobName, DRIVER_CLASS, properties, _logger);
    final String runMethod = properties.getProperty(RUN_METHOD_PARAM, DEFAULT_RUN_METHOD);
    System.setProperty(ENV,"dev");
    runMethod(_javaObject, runMethod);
  }

  private Logger createLogger() {
    Logger _logger;
    _logger = Logger.getRootLogger();
    _logger.removeAllAppenders();
    ConsoleAppender appender = new ConsoleAppender(DEFAULT_LAYOUT);
    appender.activateOptions();
    _logger.addAppender(appender);
    return _logger;
  }

  public static void main(String[] args) throws Exception {
    @SuppressWarnings("unused")
    AzkabanJobRunner wrapper = new AzkabanJobRunner();
  }

  private static Object getObject(String jobName, String className,
                                  Properties properties, Logger logger) throws Exception {

    Class<?> runningClass = AzkabanJobRunner.class.getClassLoader().loadClass(className);

    if (runningClass == null) {
      throw new Exception("Class " + className
          + " was not found. Cannot run job.");
    }

    Class<?> propsClass = null;
    for (String propClassName : PROPS_CLASSES) {
      try {
        propsClass = AzkabanJobRunner.class.getClassLoader().loadClass(propClassName);
      } catch (ClassNotFoundException e) {
      }

      if (propsClass != null
          && getConstructor(runningClass, String.class, propsClass) != null) {
        // is this the props class
        break;
      }
      propsClass = null;
    }

    Object obj = null;
    if (propsClass != null
        && getConstructor(runningClass, String.class, propsClass) != null) {
      // Create props class
      Constructor<?> propsCon =
          getConstructor(propsClass, propsClass, Properties[].class);
      Object props =
          propsCon.newInstance(null, new Properties[]{properties});

      Constructor<?> con =
          getConstructor(runningClass, String.class, propsClass);
      logger.info("Constructor found " + con.toGenericString());
      obj = con.newInstance(jobName, props);
    } else if (getConstructor(runningClass, String.class, Properties.class) != null) {
      Constructor<?> con =
          getConstructor(runningClass, String.class, Properties.class);
      logger.info("Constructor found " + con.toGenericString());
      obj = con.newInstance(jobName, properties);
    } else if (getConstructor(runningClass, String.class, Map.class) != null) {
      Constructor<?> con =
          getConstructor(runningClass, String.class, Map.class);
      logger.info("Constructor found " + con.toGenericString());

      HashMap<Object, Object> map = new HashMap<Object, Object>();
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        map.put(entry.getKey(), entry.getValue());
      }
      obj = con.newInstance(jobName, map);
    } else if (getConstructor(runningClass, String.class) != null) {
      Constructor<?> con = getConstructor(runningClass, String.class);
      logger.info("Constructor found " + con.toGenericString());
      obj = con.newInstance(jobName);
    } else if (getConstructor(runningClass) != null) {
      Constructor<?> con = getConstructor(runningClass);
      logger.info("Constructor found " + con.toGenericString());
      obj = con.newInstance();
    } else {
      logger.error("Constructor not found. Listing available Constructors.");
      for (Constructor<?> c : runningClass.getConstructors()) {
        logger.info(c.toGenericString());
      }
    }
    return obj;
  }

  private static Constructor<?> getConstructor(Class<?> c, Class<?>... args) {
    try {
      Constructor<?> cons = c.getConstructor(args);
      return cons;
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  private void runMethod(Object obj, String runMethod)
      throws IllegalAccessException, InvocationTargetException,
      NoSuchMethodException {
    obj.getClass().getMethod(runMethod, new Class<?>[]{}).invoke(obj);
  }

}
