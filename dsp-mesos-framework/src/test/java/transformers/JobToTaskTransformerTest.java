package transformers;

import com.flipkart.dsp.mesos.constants.ResourceConstants;
import com.flipkart.dsp.mesos.entities.Job;
import com.flipkart.dsp.mesos.exceptions.TransformationException;
import com.flipkart.dsp.mesos.transformers.JobToTaskTransformer;
import org.apache.mesos.Protos;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JobToTaskTransformerTest {

    JobToTaskTransformer jobToTaskTransformer = new JobToTaskTransformer();

    @Test
    public void transform() throws TransformationException {
        List<String> uris = new ArrayList<>();
        uris.add("file:///tmp/haha");
        Job.Volume volume = new Job.Volume("/tmp/", "/tmp/", Job.MountMode.RO);
        List<Job.Volume> volumeList = new ArrayList<>();
        volumeList.add(volume);
        Job job = new Job(1.0, 1024, "set -x;cd /tmp/haha;cat /tmp/script.sh", "0.0.0.0:5000/debian:jessie-slim", "DOCKER", "BRIDGE", null, uris, volumeList, 3, "id1", "name1", true);
        job.setSlaveID(Protos.SlaveID.newBuilder().setValue("slave1").build());
        job.setFrameworkID(Protos.FrameworkID.newBuilder().setValue("framework1").build());
        job.setSlaveIp("0.0.0.0");

        Protos.TaskID taskID = Protos.TaskID.newBuilder()
                .setValue(job.getId())
                .build();
        Protos.TaskInfo.Builder taskInfoBuilder = Protos.TaskInfo.newBuilder();
        taskInfoBuilder.setName(job.getName());
        taskInfoBuilder.setTaskId(taskID);


        taskInfoBuilder.addResources(Protos.Resource.newBuilder()
                .setName(ResourceConstants.CPUS)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                        .setValue(job.getCpus())
                        .build())
                .setRevocable(Protos.Resource.RevocableInfo.newBuilder().buildPartial())
                .build());

        taskInfoBuilder.addResources(Protos.Resource.newBuilder()
                .setName(ResourceConstants.MEMORY)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder()
                        .setValue(job.getMem())
                        .build())
                .setRevocable(Protos.Resource.RevocableInfo.newBuilder().buildPartial())
                .build());

        Protos.CommandInfo.Builder commandInfoBuilder = Protos.CommandInfo.newBuilder();
        commandInfoBuilder.setValue(job.getCommand() + " " + job.getSlaveID().getValue() + " "
                + job.getFrameworkID().getValue() + " " + job.getId() + " " + job.getSlaveIp() + " "
                + job.getCpus() + " " +job.getMem() + " " + job.getAttempt() + " "
                + job.retrivePreviousAttemptContainerID(job.getAttempt()));

        Set<Protos.CommandInfo.URI> uriSet = new HashSet<>();
        job.getUris().forEach(uri -> {
            uriSet.add(Protos.CommandInfo.URI.newBuilder().setValue(uri).build());
        });
        commandInfoBuilder.addAllUris(uriSet);

        taskInfoBuilder.setCommand(commandInfoBuilder.build());
        taskInfoBuilder.setSlaveId(job.getSlaveID());

        Protos.ContainerInfo.Builder containerInfoBuilder = Protos.ContainerInfo.newBuilder();
        containerInfoBuilder.setType(Protos.ContainerInfo.Type.valueOf(job.getContainerType()));
        containerInfoBuilder.setDocker(Protos.ContainerInfo.DockerInfo.newBuilder()
                .setNetwork(Protos.ContainerInfo.DockerInfo.Network.valueOf(job.getContainerNetwork()))
                .setImage(job.getImageLabel())
                .build());

        for (Job.Volume tempVolume : job.getVolumes()) {
            containerInfoBuilder.addVolumes(Protos.Volume.newBuilder()
                    .setContainerPath(tempVolume.getContainerPath())
                    .setHostPath(tempVolume.getHostPath())
                    .setMode(Protos.Volume.Mode.valueOf(tempVolume.getMountMode().name()))
                    .build());
        }
        taskInfoBuilder.setContainer(containerInfoBuilder.build());

        Protos.TaskInfo expectedTaskInfo = taskInfoBuilder.build();

        Protos.TaskInfo actualTaskInfo = jobToTaskTransformer.transform(job);
        Assert.assertEquals(expectedTaskInfo, actualTaskInfo);

    }

    @Test(expected = TransformationException.class)
    public void transformFail() throws TransformationException {
        Job job = new Job(1.0, 1024, "set -x;cd /tmp/haha;cat /tmp/script.sh", "0.0.0.0:5000/debian:jessie-slim", "DOCKER", "BRIDGE", null, null, null, 3, "id1", "name1", true);
        jobToTaskTransformer.transform(job);
    }
}
