package com.flipkart.dsp.mesos;

import com.flipkart.dsp.mesos.entities.Job;

/**
 */
public class MesosConstants {
    public static final String CONTAINER_NETWORK = "BRIDGE";
    public static final String CONTAINER_TYPE = "DOCKER";
    public static final Job.MountMode MOUNT_MODE = Job.MountMode.RO;
}
