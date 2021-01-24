package com.flipkart.dsp.mesos.schedulers;

import com.flipkart.dsp.mesos.entities.Job;
import com.flipkart.dsp.mesos.entities.JobGroup;
import com.flipkart.dsp.mesos.exceptions.SchedulerException;
import com.flipkart.dsp.mesos.exceptions.TransformationException;
import com.flipkart.dsp.mesos.transformers.JobToTaskTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class FirstFitScheduler implements Scheduler {
    private List<JobGroup> jobGroupList;
    private FirstFitSchedulingAlgorithm firstFitSchedulingAlgorithm = new FirstFitSchedulingAlgorithm();
    private JobToTaskTransformer jobToTaskTransformer = new JobToTaskTransformer();
    private Boolean failFast;

    public FirstFitScheduler(List<JobGroup> jobGroupList, Boolean failFast) {
        this.jobGroupList = jobGroupList;
        this.failFast = failFast;
    }

    @Override
    public void registered(SchedulerDriver schedulerDriver, FrameworkID frameworkID, MasterInfo masterInfo) {
        log.info("Registered with FrameworkId :{} through master {}:{}", frameworkID.getValue(), masterInfo.getAddress().getIp(), masterInfo.getPort());
    }

    @Override
    public void statusUpdate(SchedulerDriver schedulerDriver, TaskStatus taskStatus) {
        synchronized (jobGroupList) {
            Map<String, Job> jobMap = JobGroup.getActiveJobs(jobGroupList);
            if (!jobMap.containsKey(taskStatus.getTaskId().getValue())) {
                throw new IllegalStateException("Status update received for job id:" + taskStatus.getTaskId().getValue() + " for which there is no entry in job Map:" + jobMap);
            }
            Job job = jobMap.get(taskStatus.getTaskId().getValue());

            switch (taskStatus.getState()) {
                case TASK_RUNNING:
                    job.started();
                    log.debug("updated attempt {} with container ID : {}",job.getAttempt(),taskStatus.getContainerStatus().getContainerId().getValue());
                    job.updateAttempt(taskStatus.getContainerStatus().getContainerId().getValue());
                    break;
                case TASK_FINISHED:
                    job.succeed();
                    break;
                case TASK_FAILED:
                case TASK_KILLED:
                case TASK_LOST:
                case TASK_ERROR:
                    log.warn("Job with id {} failed because of the following reason: {} ,Reason code: {}, with mem {}", job.getId(), taskStatus.getMessage(), taskStatus.getReason(), job.getMem());
                    job.fail();
                    //todo: remove this hack once the Resource computation logic is implemented.
                    if (taskStatus.getReason().getNumber() == 0) {
                        double newMem = job.getMem() * 2;
                        //Max memory that is allowed to allocate
                        if (job.getMem() * 2 > 94000) {
                            newMem = 94000;
                        }
                        job.setMem(newMem);
                        log.warn("Updated memory to {}", newMem);
                    }
                    break;
                default:
                    break;
            }
            log.info("Job list Status: Pending: {}, Staged: {}, Running: {}, Successful: {}, Failed: {}"
                    , JobGroup.getJobStatusCount(jobGroupList, Job.Status.PENDING)
                    , JobGroup.getJobStatusCount(jobGroupList, Job.Status.STAGING)
                    , JobGroup.getJobStatusCount(jobGroupList, Job.Status.RUNNING)
                    , JobGroup.getJobStatusCount(jobGroupList, Job.Status.SUCCESSFUL)
                    , JobGroup.getJobStatusCount(jobGroupList, Job.Status.FAILED));
        }
    }

    @Override
    public void error(SchedulerDriver schedulerDriver, String s) {
        log.error("Error received from driver: {}", s);
    }

    @Override
    public void disconnected(SchedulerDriver schedulerDriver) {
        log.warn("Driver disconnected from unknown reason.");
    }

    @Override
    public void slaveLost(SchedulerDriver schedulerDriver, SlaveID slaveID) {
        log.warn("Slave Id with id {} lost!", slaveID.getValue());
    }

    @Override
    public void executorLost(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, int i) {
        log.warn("Executor with executor id {} in slave with id {} lost!", executorID.getValue(), slaveID.getValue());
    }

    @Override
    public void frameworkMessage(SchedulerDriver schedulerDriver, ExecutorID executorID, SlaveID slaveID, byte[] bytes) {
        //not making use of messages.
    }

    @Override
    public void reregistered(SchedulerDriver schedulerDriver, MasterInfo masterInfo) {
        log.warn("Framework re-registered through master {}:{}", masterInfo.getAddress().getIp(), masterInfo.getPort());
    }

    @Override
    public void offerRescinded(SchedulerDriver schedulerDriver, OfferID offerID) {
        log.warn("Offer with id: {} was Rescinded.", offerID.getValue());
    }

    @Override
    public void resourceOffers(SchedulerDriver schedulerDriver, List<Offer> offerList) {
        synchronized (jobGroupList) {
            List<Job> unfinishedJobs = new ArrayList<>(JobGroup.getActiveJobs(jobGroupList).values());

            long failedJobCount = JobGroup.getJobStatusCount(jobGroupList, Job.Status.FAILED);

            if (failedJobCount>0 && failFast) {
                log.info("Found job failures, Failing Fast!");
                offerList.forEach(offer -> schedulerDriver.declineOffer(offer.getId()));
                schedulerDriver.stop();
            }

            if (unfinishedJobs.isEmpty()) {
                //If all jobs are finished then decline all offers and stop the scheduler.
                log.info("All jobs are finished!");
                offerList.forEach(offer -> schedulerDriver.declineOffer(offer.getId()));
                schedulerDriver.stop();
            }
            List<Job> pendingJobList = unfinishedJobs.stream().filter(job -> job.getStatus().equals(Job.Status.PENDING)).collect(Collectors.toList());

            Map<Offer, List<Job>> offerToScheduledJobs = firstFitSchedulingAlgorithm.schedule(offerList, pendingJobList);
            offerToScheduledJobs.forEach((offer, localJobList) -> {
                if (localJobList.isEmpty()) {
                    schedulerDriver.declineOffer(offer.getId());
                } else {
                    final List<TaskInfo> taskInfoList = new ArrayList<>();
                    localJobList.forEach(job -> {
                        job.setSlaveID(offer.getSlaveId());
                        job.setFrameworkID(offer.getFrameworkId());
                        job.setSlaveIp(offer.getUrl().getAddress().getIp());
                        try {
                            taskInfoList.add(jobToTaskTransformer.transform(job));
                        } catch (TransformationException e) {
                            throw new SchedulerException("Scheduler failed due to transformation Exception", e);
                        }
                    });
                    log.info("Scheduling {} jobs", taskInfoList.size());
                    schedulerDriver.launchTasks(Collections.singletonList(offer.getId()), taskInfoList);
                    localJobList.forEach(Job::launch);
                }
            });
        }
    }
}
