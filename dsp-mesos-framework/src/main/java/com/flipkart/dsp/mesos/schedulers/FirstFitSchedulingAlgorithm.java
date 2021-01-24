package com.flipkart.dsp.mesos.schedulers;

import com.flipkart.dsp.mesos.constants.ResourceConstants;
import com.flipkart.dsp.mesos.entities.Job;
import com.flipkart.dsp.mesos.exceptions.SchedulerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.Protos;

import java.util.*;

@Slf4j
public class FirstFitSchedulingAlgorithm extends SchedulingAlgorithm {

    @Override
    public Map<Protos.Offer, List<Job>> schedule(List<Protos.Offer> offerList, List<Job> jobList) {
        if (offerList == null || jobList == null) {
            throw new SchedulerException("Null Job list or offer list provided for scheduling algorithm");
        }

        Map<Protos.Offer, List<Job>> scheduledJobs = new HashMap<>();

        sortJobsAndOffersOnCpu(offerList,jobList);

        List<Job> pendingJobList = new ArrayList<>();
        pendingJobList.addAll(jobList);

        for (Protos.Offer offer: offerList) {
            if (pendingJobList.isEmpty()) {
                scheduledJobs.put(offer,new ArrayList<>());
            }
            List<Job> currentScheduledJobs = doFirstFit(offer,pendingJobList);
            scheduledJobs.put(offer,currentScheduledJobs);
            pendingJobList.removeAll(currentScheduledJobs); //update pending jobs list;
        }

        return scheduledJobs;
    }

    private void sortJobsAndOffersOnCpu(List<Protos.Offer> offerList, List<Job> jobList) {
        offerList.sort(new Comparator<Protos.Offer>() {
            @Override
            public int compare(Protos.Offer o1, Protos.Offer o2) {
                //Sorting based on cpu as this is the primary resource for us.
                double offerCpus1 = 0;
                double offerCpus2 = 0;

                for (Protos.Resource r : o1.getResourcesList()) {
                    if (r.getName().equals(ResourceConstants.CPUS)) {
                        offerCpus1 += r.getScalar().getValue();
                    }
                }

                for (Protos.Resource r : o2.getResourcesList()) {
                    if (r.getName().equals(ResourceConstants.CPUS)) {
                        offerCpus2 += r.getScalar().getValue();
                    }
                }

                return Double.compare(offerCpus1,offerCpus2);
            }
        });

        jobList.sort(new Comparator<Job>() {
            @Override
            public int compare(Job o1, Job o2) {
                return Double.compare(o1.getCpus(),o2.getCpus());
            }
        });
    }

    private List<Job> doFirstFit(Protos.Offer offer, List<Job> pendingJobs) {
        List<Job> scheduledJobs = new ArrayList<>();
        double offerCpus = 0;
        double offerMem = 0;
        boolean memRevocable = false;
        boolean cpuRevocable = false;

        for (Protos.Resource r : offer.getResourcesList()) {
            if (r.getName().equals(ResourceConstants.CPUS)) {
                if (r.hasRevocable()) {
                    cpuRevocable = true;
                }
                offerCpus += r.getScalar().getValue();
            } else if (r.getName().equals(ResourceConstants.MEMORY)) {
                offerMem += r.getScalar().getValue();
                if (r.hasRevocable()) {
                    memRevocable = true;
                }
            }
        }

        for (Job job : pendingJobs) {
            double jobCpus = job.getCpus();
            double jobMem = job.getMem();
            if (jobCpus <= offerCpus && jobMem <= offerMem && !(!job.isPreEmptable() && (cpuRevocable || memRevocable))) {
                offerCpus -= jobCpus;
                offerMem -= jobMem;
                //My job is revocable, but cluster is offering non-revocable resources. I accept with thanks.
                if (job.isPreEmptable() && !cpuRevocable && !memRevocable) {
                    job.setPreEmptable(false);
                }
                scheduledJobs.add(job);
            } else {
                break;
            }
        }

        return scheduledJobs;
    }
}
