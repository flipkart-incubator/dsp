package com.flipkart.dsp.mesos.entities;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.Protos;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@NoArgsConstructor
@Getter
@EqualsAndHashCode
@JsonAutoDetect
@JsonIgnoreProperties(ignoreUnknown = true)
public class Job implements Serializable {
    public enum MountMode {
        RO,
        RW
    }

    public enum Status {
        PENDING,
        STAGING,
        RUNNING,
        SUCCESSFUL,
        FAILED
    }

    @Getter
    @NoArgsConstructor
    @EqualsAndHashCode
    public static class Volume {
        private String hostPath;
        private String containerPath;
        private MountMode mountMode;

        public Volume(String hostPath, String containerPath, MountMode mountMode) {
            this.hostPath = hostPath;
            this.containerPath = containerPath;
            this.mountMode = mountMode;
        }
    }

    private double cpus;
    @Setter
    private double mem;
    private String command;
    private String imageLabel;
    private String containerType;
    private String containerNetwork;
    private Map<String, String> containerOptions = new HashMap<>();
    private List<String> uris;
    private Status status = Status.PENDING;
    private List<Volume> volumes;

    private int retries;
    private String id;
    private String name;
    @Setter
    private boolean preEmptable;


    @Setter
    @JsonIgnore
    private Protos.SlaveID slaveID;

    @Setter
    @JsonIgnore
    private String slaveIp;

    @Setter
    @JsonIgnore
    private Protos.FrameworkID frameworkID;

    private Integer attempt = 0;
    private Map<Integer, String> attemptRunMap = new HashMap<>();

    public Job(double cpus, double mem, String command, String imageLabel, String containerType, String containerNetwork,Map<String, String>  containerOptions, List<String> uris, List<Volume> volumes, int retries, String id, String name, boolean preEmptable) {
        this.cpus = cpus;
        this.mem = mem;
        this.command = command;
        this.imageLabel = imageLabel;
        this.containerType = containerType;
        this.containerNetwork = containerNetwork;
        this.uris = uris;
        this.volumes = volumes;
        this.retries = retries;
        this.id = id;
        this.name = name;
        this.status = Status.PENDING;
        this.preEmptable = preEmptable;
        if (containerOptions == null) {
            this.containerOptions = new HashMap<>();
        } else {
            this.containerOptions = containerOptions;
        }
    }

    public void launch() {
        this.status = Status.STAGING;
    }

    public void started() {
        this.status = Status.RUNNING;
    }

    public void succeed() {
        this.status = Status.SUCCESSFUL;
    }

    public void fail() {
        if (retries == 0) {
            this.status = Status.FAILED;
        } else {
            retries--;
            this.status = Status.PENDING;
        }
    }

    public void updateAttempt(String containerId) {
        this.attemptRunMap.put(attempt, containerId);
        this.attempt++;
    }

    public String retrivePreviousAttemptContainerID(Integer attempt) {
        return attemptRunMap.getOrDefault(attempt - 1, "latest");
    }

}
