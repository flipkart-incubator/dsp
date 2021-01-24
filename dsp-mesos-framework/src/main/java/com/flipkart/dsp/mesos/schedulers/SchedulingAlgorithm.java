package com.flipkart.dsp.mesos.schedulers;

import com.flipkart.dsp.mesos.entities.Job;
import org.apache.mesos.Protos;

import java.util.List;
import java.util.Map;

abstract public class SchedulingAlgorithm {
    abstract public Map<Protos.Offer,List<Job>> schedule(List<Protos.Offer> offerList, List<Job> jobList);
}
