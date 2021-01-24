package com.flipkart.dsp.mesos;

import com.flipkart.dsp.actors.ExecutionEnvironmentSnapShotActor;
import com.flipkart.dsp.config.DSPServiceConfig;
import com.flipkart.dsp.entities.misc.ImageDetailPayload;
import com.flipkart.dsp.mesos.entities.Job;
import com.flipkart.dsp.mesos.entities.JobGroup;
import com.flipkart.dsp.mesos.framework.DSPMesosFramework;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ImageDetailsMesosExecutionDriver {

    private final DSPServiceConfig dspServiceConfig;
    private final ExecutionEnvironmentSnapShotActor executionEnvironmentSnapshotActor;


    public void execute(List<ExecutionEnvironmentSummary> executionEnvironmentSummaries) {
        if (executionEnvironmentSummaries.isEmpty()) {
            log.info("No image got updated from last run, stopping job.");
            return;
        }

        log.info(String.format("Total %s image got updated from last run", executionEnvironmentSummaries.size()));
        String mesosZookeeperAddress = dspServiceConfig.getMesosConfig().getZookeeperAddress();
        DSPMesosFramework dspMesosFramework = new DSPMesosFramework(mesosZookeeperAddress, Constants.EXPERIMENTATION_MESOS_QUEUE, "GetImageDetails", true);
        List<JobGroup> jobGroups = populateJobGroups(executionEnvironmentSummaries);
        dspMesosFramework.run(jobGroups, false);
    }

    private List<JobGroup> populateJobGroups(List<ExecutionEnvironmentSummary> executionEnvironmentSummaries) {
        ArrayList<Job> jobList = new ArrayList<>();
        List<JobGroup> jobGroups = new ArrayList<>();
        for (ExecutionEnvironmentSummary environment : executionEnvironmentSummaries) {
            String command = generateCommand(environment);
            Map<String, String> mesosContainerOptions = dspServiceConfig.getMesosConfig().getContainerOptions();
            Job job = new Job(dspServiceConfig.getImageSnapShotConfig().getCpus(), dspServiceConfig.getImageSnapShotConfig().getMemory(), command,
                    environment.getImagePath(), MesosConstants.CONTAINER_TYPE, MesosConstants.CONTAINER_NETWORK, mesosContainerOptions, null, getMountVolumes(),
                    dspServiceConfig.getImageSnapShotConfig().getRetires(), UUID.randomUUID().toString(), UUID.randomUUID().toString(), true);
            jobList.add(job);
        }
        jobGroups.add(new JobGroup(jobList));
        return jobGroups;
    }

    private String generateCommand(ExecutionEnvironmentSummary executionEnvironmentSummary) {
        String commandPrefix = "bash " + executionEnvironmentSummary.getImagePath() + " ";
        ImageDetailPayload imageDetailPayload = ImageDetailPayload.builder()
                .executionEnvironmentSummary(executionEnvironmentSummary)
                .executionEnvironmentSnapshots(executionEnvironmentSnapshotActor.getAllSnapShots(executionEnvironmentSummary.getId())).build();
        return commandPrefix + getExecutionDriverName() + " " + System.getProperty(Constants.CONFIG_SVC_BUCKETS_KEY) +
                " \"" + StringEscapeUtils.escapeJava(JsonUtils.DEFAULT.toJson(imageDetailPayload)) + "\"";
    }

    private String getExecutionDriverName() {
        return Constants.IMAGE_DETAILS_MESOS_APPLICATION;
    }

    private List<Job.Volume> getMountVolumes() {
        List<Job.Volume> mountVolumes = new ArrayList<>();
        for (Map.Entry<String, String> entry : dspServiceConfig.getMesosConfig().getMountInfo().entrySet()) {
            mountVolumes.add(new Job.Volume(entry.getKey(), entry.getValue(), MesosConstants.MOUNT_MODE));
        }
        return mountVolumes;
    }
}
