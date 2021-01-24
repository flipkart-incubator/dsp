package com.flipkart.dsp.mesos.application;

import com.flipkart.dsp.mesos.entities.JobGroup;
import com.flipkart.dsp.mesos.exceptions.TransformationException;
import com.flipkart.dsp.mesos.framework.DSPMesosFramework;
import com.flipkart.dsp.mesos.transformers.StringToJobGroupTransformer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
public class DSPMesosApplication {

    StringToJobGroupTransformer stringToJobGroupTransformer = new StringToJobGroupTransformer();

    public static void main(String[] args) throws IOException, TransformationException {

        if (args.length != 5) {
            log.info("{} : {}",args,args.length);
            log.info("Usage: DSPMesosApplication <zkAddress> <user> <frameworkName> <jobsFilePath> <preEmptable>");
            System.exit(0);
        }

        String zkAddress = args[0];
        String role = args[1];
        String frameworkName = args[2];
        String filePath = args[3];
        boolean preEmptable = Boolean.valueOf(args[4]);

        DSPMesosFramework dspMesosFramework = new DSPMesosFramework(zkAddress, role, frameworkName, preEmptable);

        DSPMesosApplication dspMesosApplication = new DSPMesosApplication();
        List<JobGroup> jobList = dspMesosApplication.getJobs(filePath);
        dspMesosFramework.run(jobList, true);

    }

    private List<JobGroup> getJobs(String path) throws IOException, TransformationException{
        byte[] fileContents = Files.readAllBytes(Paths.get(path));
        return stringToJobGroupTransformer.transformList(new String(fileContents));
    }
}
