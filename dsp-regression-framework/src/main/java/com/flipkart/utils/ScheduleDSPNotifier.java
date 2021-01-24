package com.flipkart.utils;

import com.flipkart.dsp.azkaban.AzkabanCreateProjectResponse;
import com.flipkart.dsp.azkaban.AzkabanLoginResponse;
import com.flipkart.dsp.azkaban.AzkabanProjectScheduleRequestResponse;
import com.flipkart.dsp.client.AzkabanClient;
import com.flipkart.dsp.config.AzkabanConfig;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.exceptions.AzkabanException;
import com.flipkart.exception.FileOperationException;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import static com.flipkart.dsp.utils.Constants.ZIP_EXTENSION;
import static java.lang.String.format;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ScheduleDSPNotifier {
    private final MiscConfig miscConfig;
    private final AzkabanClient azkabanClient;
    private final AzkabanConfig azkabanConfig;
    private final FileOperationsHelper fileOperationsHelper;

    private static final String BUCKET = "BUCKET";
    private static final String ENVIRONMENT = "ENVIRONMENT";
    private static final String JAR_VERSION = "JAR_VERSION";
    private static final String JOB_EXT = ".job";
    private static final String PROJECT_NAME = "NotifierNode";
    private static final String AZKABAN_HOST_IP = "AZKABAN_HOST_IP";


    public void scheduleNotifier(String bucketPostfix, String azkabanPackageVersion) throws AzkabanException {
        String sessionId = getSessionId();
        createNotifierProject(sessionId);
        String path = createNotifierJobFile(bucketPostfix, azkabanPackageVersion);
        path = createZipFile(path);
        uploadProject(sessionId, path, PROJECT_NAME);
        try {
            AzkabanProjectScheduleRequestResponse response = scheduleProject(sessionId);
            log.info("Schedule Job Notifier Job Status " + response.getStatus() + "  " + response.getMessage());
            if(!response.getStatus().equals("success")) {
                throw new AzkabanException("Failed to Schedule Notifier Node");
            }
        } catch (Exception e) {
            throw new AzkabanException("Issue in Scheduling Notifier Job " + e.getMessage());
        }
    }

    private AzkabanProjectScheduleRequestResponse scheduleProject(String sessionId) throws AzkabanException {
        String pattern = "MM/dd/yyyy";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        String date = simpleDateFormat.format(new Date());
        return azkabanClient.scheduleProject("on", "10m", PROJECT_NAME, "Notifier", 1L,
                "00,00,am,PDT", date , sessionId);

    }

    private String createZipFile(String path) {
        String randomUUID = UUID.randomUUID().toString();
        Path zipFile = Paths.get(path, randomUUID + ZIP_EXTENSION);
        ZipUtil.pack(new File(path), zipFile.toFile());
        return zipFile.toString();
    }

    private String createNotifierJobFile(String bucketPostfix, String azkabanPackageVersion) throws AzkabanException {
        try {
            String runFolderName = String.valueOf(new Date().getTime());
            String userHomeDir = System.getProperty("user.home");
            String tmpFolderPath = String.format(DSPConstants.LOCAL_FOLDER_LOCATION, userHomeDir, runFolderName);
            fileOperationsHelper.createDirectoryIfNotExist(tmpFolderPath);
            String fileContents = IOUtils.toString(
                    (InputStream) this.getClass().getResource("/fixtures/dsp/azkaban/Notifier.job").getContent()
                    , StandardCharsets.UTF_8.toString()) ;
            fileContents = fileContents.replaceAll(ENVIRONMENT, miscConfig.getEnvironment());
            fileContents = fileContents.replaceAll(BUCKET, format(DSPConstants.DSP_CONFIG_BUCKET_FORMAT, bucketPostfix));
            fileContents = fileContents.replaceAll(JAR_VERSION, azkabanPackageVersion);
            fileContents = fileContents.replaceAll(AZKABAN_HOST_IP, azkabanConfig.getHost());
            final String jobName = "Notifier";
            Path path = Paths.get(tmpFolderPath, jobName + JOB_EXT);
            FileUtils.writeStringToFile(new File(path.toUri()), fileContents);
            return tmpFolderPath;
        } catch (IOException | FileOperationException e) {
            throw new AzkabanException("unable to create Azkaban Notifier job File", e);
        }
    }

    private void uploadProject(String sessionId, String path, String projectName) throws AzkabanException {
        try {
            azkabanClient.uploadProject(sessionId, projectName, path);
        } catch (AzkabanException e) {
            throw new AzkabanException("Error in Uploading notifier project ", e);
        }
    }

    private void createNotifierProject(String sessionId) throws AzkabanException {
        AzkabanCreateProjectResponse azkabanCreateProjectResponse = azkabanClient.createProject(sessionId,
                "NotifierNode",
                "Schedules Azkaban job");
        if(azkabanCreateProjectResponse.getStatus().equals("error") &&
                !(azkabanCreateProjectResponse.getMessage() != null &&
                        azkabanCreateProjectResponse.getMessage().contains(" already exists in db."))) {
            throw new AzkabanException("Unable to create Notifier node " + azkabanCreateProjectResponse.getMessage());
        }

    }

    private String getSessionId() throws AzkabanException {
        try {
            AzkabanLoginResponse azkabanLoginResponse = azkabanClient.getAzkabanLoginRequest().azkabanConfig(azkabanConfig).executeSync();
            return azkabanLoginResponse.getSessionId();
        } catch (Exception e) {
            log.error("exception in getting azkaban session id", e);
            throw new AzkabanException("Unable to get Azkaban Session id ", e);
        }
    }
}
