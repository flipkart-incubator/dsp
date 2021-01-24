package com.flipkart.dsp.mesos.framework;

import com.flipkart.dsp.mesos.entities.JobGroup;
import com.flipkart.dsp.mesos.schedulers.FirstFitScheduler;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import java.util.List;

@Slf4j
public class DSPMesosFramework {

    private final String masterZkAdderss;
    private final FrameworkInfo frameworkInfo;

    public DSPMesosFramework(String masterZkAdderss, String role, String name, boolean preEmptable) {
        this.masterZkAdderss = masterZkAdderss;
        if (role == null) {
            role = "*";
        }
        FrameworkInfo.Builder frameworkInfoBuilder = FrameworkInfo.newBuilder()
                .setUser("fk-idf-dev")
                .setName(name)
                .setRole(role);

        if (preEmptable) {
            frameworkInfoBuilder.addCapabilities(FrameworkInfo.Capability.newBuilder().setType(FrameworkInfo.Capability.Type.REVOCABLE_RESOURCES).build());
        }
        this.frameworkInfo = frameworkInfoBuilder.build();
    }

    public List<JobGroup> run(List<JobGroup> jobGroupList, Boolean failFast) {

        Scheduler myScheduler = new FirstFitScheduler(jobGroupList, failFast);
        SchedulerDriver schedulerDriver = new MesosSchedulerDriver(myScheduler, frameworkInfo, masterZkAdderss);

        log.info("Driver Starting");
        schedulerDriver.start();

        //todo: Add reconciliation here

        schedulerDriver.join();
        log.info("Driver joined");

        return jobGroupList;
    }

}
