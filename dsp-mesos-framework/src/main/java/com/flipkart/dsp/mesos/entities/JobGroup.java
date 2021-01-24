package com.flipkart.dsp.mesos.entities;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.*;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@JsonAutoDetect
@JsonIgnoreProperties(ignoreUnknown = true)
public class JobGroup {
    public ArrayList<Job> jobList;

    public Optional<Job> getNextRunnableJob() {
        if (hasFailures()) {
            return Optional.empty();
        } else {
            return jobList.stream().filter(job -> !job.getStatus().equals(Job.Status.SUCCESSFUL)).findFirst();
        }
    }

    //returns true if jobGroup has failed jobs
    public boolean hasFailures() {
        return jobList.stream().anyMatch(job -> job.getStatus().equals(Job.Status.FAILED));
    }

    public boolean hasFinished() {
        return jobList.stream().allMatch(job -> job.getStatus().equals(Job.Status.SUCCESSFUL));
    }

    public long getJobStatusCount(final Job.Status status) {
        return jobList.stream().filter(job -> job.getStatus().equals(status)).count();
    }

    public static Map<String, Job> getActiveJobs(List<JobGroup> jobGroupList) {
        Map<String, Job> jobMap = new HashMap<>();
        jobGroupList.forEach(jobGroup -> {
            jobGroup.getNextRunnableJob().ifPresent(job -> jobMap.put(job.getId(), job));
        });
        return jobMap;
    }

    public static long getJobStatusCount(List<JobGroup> jobGroupList, final Job.Status status) {
        return jobGroupList.stream().mapToLong(jobGroup -> jobGroup.getJobStatusCount(status)).sum();
    }
}
