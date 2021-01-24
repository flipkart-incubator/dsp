package com.flipkart.dsp.service;

import com.flipkart.dsp.api.AzkabanExecutionAPI;
import com.flipkart.dsp.config.AzkabanConfig;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.dto.AzkabanFlow;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exception.AzkabanProjectCreationException;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.dsp.exceptions.ConfigServiceException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.utils.URIBuilder;
import org.zeroturnaround.zip.ZipUtil;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.UUID;

import static com.flipkart.dsp.utils.Constants.*;
import static java.lang.String.format;
import static java.lang.String.valueOf;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@com.google.inject.Inject))
public class AzkabanProjectHelper {

    private static final String JOB_EXT = ".job";
    private static final String BUCKET = "BUCKET";
    private static final String WORKFLOW = "WORKFLOW";
    private static final String DEPENDENCY = "DEPENDENCY";
    private static final String PROJECT_NAME = "%s_%s_%s";
    private static final String DRAFT_PROJECT = "DRAFT_%s";
    private static final String ENVIRONMENT = "ENVIRONMENT";
    private static final String JAR_VERSION = "JAR_VERSION";
    private static final String BUCKET_FORMAT = "dsp-%s";
    private static final String AZKABAN_HOST_IP = "AZKABAN_HOST_IP";
    private static final String AZKABAN_JOB_RESOURCE_PATH = "/azkaban_job_templates/";

    private final MiscConfig miscConfig;
    private final AzkabanConfig azkabanConfig;
    private final AzkabanExecutionAPI azkabanExecutionAPI;

    public AzkabanFlow setupAzkabanJob(WorkflowDetails workflowDetails) throws AzkabanProjectCreationException {
        Workflow workflow = workflowDetails.getWorkflow();
        String projectName = getAzkabanProjectName(workflow.getName(), workflow.getVersion(), workflow.getIsProd());
        log.info("Creating and uploading Azkaban project {}", projectName);

        final String tempDir = miscConfig.getTempDir();
        final String randomUUID = UUID.randomUUID().toString();
        final String basePath = tempDir + slash + randomUUID + slash;

        String flowName = createJobsForWorkflow(workflowDetails.getWorkflow(), basePath);
        Path zipFile = createZipFile(basePath, randomUUID);
        createAndUploadAzkabanProject(projectName, workflowDetails.getWorkflow().getDescription(), zipFile);
        cleanupGeneratedJobs(basePath, zipFile);
        return new AzkabanFlow(projectName, flowName);
    }

    public String getAzkabanUrl(Long azkabanExecId) {
        String ip = azkabanConfig.getElbEndPoint();
        int port = azkabanConfig.getPort();
        try {
            return new URIBuilder().setScheme("http").setHost(ip).setPort(port).setPath("executor")
                    .addParameter("execid", valueOf(azkabanExecId)).build().toString();
        } catch (URISyntaxException e) {
            //No harm done,so Simply ignore
        }
        return null;
    }


    private void createAndUploadAzkabanProject(String projectName, String description, Path zipFile) throws AzkabanProjectCreationException {

        try {
            azkabanExecutionAPI.createProject(projectName, description);
        } catch (AzkabanException e) {
            throw new AzkabanProjectCreationException("Could not create project in Azkaban : " + projectName, e);
        }

        try {
            azkabanExecutionAPI.uploadProject(projectName, zipFile.toString());
        } catch (AzkabanException e) {
            throw new AzkabanProjectCreationException("Could not upload zip file into Azkaban Project : " + projectName, e);
        }

    }

    private Path createZipFile(String basePath, String randomUUID) {
        String tempDir = miscConfig.getTempDir();
        Path zipFile = Paths.get(tempDir, randomUUID + ZIP_EXTENSION);
        ZipUtil.pack(new File(basePath), zipFile.toFile());
        return zipFile;
    }

    private String createJobsForWorkflow(Workflow workflow, String basePath) throws AzkabanProjectCreationException {
        try {
            String azkJarVersion = azkabanConfig.getJarVersion();
            //create OTS Node
            String OTSNode = createJobFile(workflow, null, null, basePath, azkJarVersion);
            String workflowNode = createJobFile(workflow, JobType.WF, OTSNode, basePath, azkJarVersion);
            String outputIngestionNode = createJobFile(workflow, JobType.OI, workflowNode, basePath, azkJarVersion);
            return createJobFile(workflow, JobType.TERMINAL, outputIngestionNode, basePath, azkJarVersion);
        } catch (IOException | ConfigServiceException e) {
            throw new AzkabanProjectCreationException("Failed to create azkaban job for workflowEntity group", e);
        }
    }

    private String createJobFile(com.flipkart.dsp.entities.workflow.Workflow workflow, JobType jobType, String dependency, String basePath, String azkJarVersion) throws IOException, ConfigServiceException {
        String environment = miscConfig.getEnvironment();
        String fileContents = IOUtils.toString(
                (InputStream) this.getClass().getResource(AZKABAN_JOB_RESOURCE_PATH + jobType.toString() + JOB_EXT).getContent()
                , StandardCharsets.UTF_8.name());
        fileContents = fileContents.replaceAll(ENVIRONMENT, environment);
        fileContents = fileContents.replaceAll(BUCKET, format(BUCKET_FORMAT, miscConfig.getBucketPostfix()));
        fileContents = fileContents.replaceAll(WORKFLOW, String.valueOf(workflow.getId()));
        fileContents = fileContents.replaceAll(DEPENDENCY, dependency);
        fileContents = fileContents.replaceAll(JAR_VERSION, azkJarVersion);
        fileContents = fileContents.replaceAll(AZKABAN_HOST_IP, azkabanConfig.getHost());
        final String jobName = workflow.getName() + "_" + jobType.toString();
        FileUtils.writeStringToFile(new File(basePath + jobName + JOB_EXT), fileContents);
        return jobName;
    }

    private String getAzkabanProjectName(String workflowName, String version, boolean isProd) {
        version = version.replace(dot, underscore);
        String projectName = format(PROJECT_NAME, workflowName, version, miscConfig.getEnvironment()).toUpperCase();
        if (isProd)
            return projectName;
        else
            return format(DRAFT_PROJECT, projectName);
    }

    private void cleanupGeneratedJobs(String basePath, Path zipFile) {
        try {
            FileUtils.deleteDirectory(new File(basePath));
            Files.delete(zipFile);
        } catch (IOException e) {
            log.warn("Job cleanup failed!!", e);
            //just some missed cleanup, no need to panic
        }
    }

    private enum JobType {
        WF, OI, TERMINAL
    }
}
