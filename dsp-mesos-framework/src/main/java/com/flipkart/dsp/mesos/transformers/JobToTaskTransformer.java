package com.flipkart.dsp.mesos.transformers;

import com.flipkart.dsp.mesos.constants.ResourceConstants;
import com.flipkart.dsp.mesos.entities.Job;
import com.flipkart.dsp.mesos.exceptions.TransformationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.Protos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class JobToTaskTransformer extends Transformer<Job, Protos.TaskInfo> {

    public Protos.TaskInfo transform(Job job) throws TransformationException {

        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder();

        //Set ID
        Protos.TaskID taskID = getTaskID(job);
        taskInfoBuilder.setTaskId(taskID);

        //Set Name
        if (job.getName() != null) {
            taskInfoBuilder.setName(job.getName());
        }

        //Set CPU Resources
        Protos.Resource cpuResource = getResourceInfo(job.isPreEmptable(), ResourceConstants.CPUS, job.getCpus());
        taskInfoBuilder.addResources(cpuResource);

        //Set Memory Resources
        Protos.Resource memoryResource = getResourceInfo(job.isPreEmptable(), ResourceConstants.MEMORY, job.getMem());
        taskInfoBuilder.addResources(memoryResource);

        //Set Command info
        Protos.CommandInfo commandInfo = getCommandInfo(job);
        taskInfoBuilder.setCommand(commandInfo);

        //Set Slave Id
        if (job.getSlaveID() == null) {
            throw new TransformationException("Slave id not found.");
        } else {
            taskInfoBuilder.setSlaveId(job.getSlaveID());
        }

        //Set container info
        Protos.ContainerInfo containerInfo = getContainerInfo(job);
        taskInfoBuilder.setContainer(containerInfo);

        return taskInfoBuilder.build();
    }

    private Protos.TaskID getTaskID(Job job) throws TransformationException {
        Protos.TaskID taskID;

        if (job.getId() == null) {
            throw new TransformationException("Id not found");
        } else {
            taskID = Protos.TaskID.newBuilder()
                    .setValue(job.getId())
                    .build();
        }
        return taskID;
    }

    private Protos.CommandInfo getCommandInfo(Job job) throws TransformationException {
        Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder();

        if (job.getCommand() == null || job.getSlaveIp() == null || job.getSlaveID() == null || job.getId() == null) {
            throw new TransformationException("Command is a mandatory to create task");
        } else {
            commandInfoBuilder.setValue(job.getCommand() + " " + job.getSlaveID().getValue() + " "
                    + job.getFrameworkID().getValue() + " " + job.getId() + " " + job.getSlaveIp() + " "
                    + job.getCpus() + " " +job.getMem() + " " + job.getAttempt() + " "
                    + job.retrivePreviousAttemptContainerID(job.getAttempt()));
        }

        //uris are optional
        if (job.getUris() != null && !job.getUris().isEmpty()) {
            Set<Protos.CommandInfo.URI> uriSet = new HashSet<>();
            job.getUris().forEach(uri -> {
                uriSet.add(Protos.CommandInfo.URI.newBuilder().setValue(uri).build());
            });
            commandInfoBuilder.addAllUris(uriSet);
        }
        return commandInfoBuilder.build();
    }

    private Protos.Resource getResourceInfo(boolean isPreEmptable, String resourceType, double resourceValue) {
        Protos.Resource.Builder resourceBuilder = Protos.Resource.newBuilder()
                .setName(resourceType)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                        .setValue(resourceValue)
                        .build());
        if (isPreEmptable) {
            resourceBuilder.setRevocable(Protos.Resource.RevocableInfo.newBuilder().build());
        }
        return resourceBuilder.build();
    }

    private Protos.ContainerInfo getContainerInfo(Job job) throws TransformationException {
        Protos.ContainerInfo.Builder containerInfoBuilder = Protos.ContainerInfo.newBuilder();
        if (job.getContainerType() == null || job.getImageLabel() == null) {
            throw new TransformationException("Incomplete container information");
        } else {
            List<Protos.Parameter> parameterList = new ArrayList<>();

            if (job.getContainerOptions() != null) {
                job.getContainerOptions().forEach((k, v) -> {
                    parameterList.add(Protos.Parameter.newBuilder().setKey(k).setValue(v).build());
                });
            }

            containerInfoBuilder.setType(Protos.ContainerInfo.Type.valueOf(job.getContainerType()));
            containerInfoBuilder.setDocker(Protos.ContainerInfo.DockerInfo.newBuilder()
                    .setNetwork(Protos.ContainerInfo.DockerInfo.Network.valueOf(job.getContainerNetwork()))
                    .setImage(job.getImageLabel())
                    .addAllParameters(parameterList)
                    .build());

            if (job.getVolumes() != null && !job.getVolumes().isEmpty()) {
                for (Job.Volume volume : job.getVolumes()) {

                    containerInfoBuilder.addVolumes(Protos.Volume.newBuilder()
                            .setContainerPath(volume.getContainerPath())
                            .setHostPath(volume.getHostPath())
                            .setMode(Protos.Volume.Mode.valueOf(volume.getMountMode().name()))
                            .build());
                }
            }
        }
        return containerInfoBuilder.build();
    }

}
