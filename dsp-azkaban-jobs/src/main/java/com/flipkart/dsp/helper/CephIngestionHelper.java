package com.flipkart.dsp.helper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.actors.ExternalCredentialsActor;
import com.flipkart.dsp.config.HadoopConfig;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.cephingestion.MultipartUploadOutputStream;
import com.flipkart.dsp.cephingestion.PartitionFileMergingDriver;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exceptions.CephIngestionException;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.exceptions.HDFSUtilsException;
import com.flipkart.dsp.models.ExternalCredentials;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.externalentities.CephEntity;
import com.flipkart.dsp.models.outputVariable.CephOutputLocation;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.utils.*;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static com.flipkart.dsp.utils.Constants.*;
import static java.util.stream.Collectors.toList;

/**
 * +
 */

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class CephIngestionHelper {
    private final HdfsUtils hdfsUtils;
    private final MiscConfig miscConfig;
    private final ObjectMapper objectMapper;
    private final HadoopConfig hadoopConfig;
    private final EventAuditUtil eventAuditUtil;
    private final ExternalCredentialsActor externalCredentialsActor;

    public List<String> ingestInCeph(Long requestId, WorkflowDetails workflowDetails) {
        List<String> errorMessages = new ArrayList<>();
        List<ScriptVariable> cephOutputs = workflowDetails.getCephOutputs();
        for (ScriptVariable scriptVariable : cephOutputs) {
            AbstractDataFrame abstractDataFrame = (AbstractDataFrame) scriptVariable.getAdditionalVariable();
            List<CephOutputLocation> cephOutputLocations = getCephOutputLocations(scriptVariable);
            for (CephOutputLocation cephOutputLocation : cephOutputLocations) {
                Workflow workflow = workflowDetails.getWorkflow();
                eventAuditUtil.createCephIngestionStartInfoEvent(requestId, workflow.getId(),
                        scriptVariable.getName(), cephOutputLocation);
                try {
                    String path = getHDFSPath(requestId, cephOutputLocation.getPath(), workflow.getName(), scriptVariable.getName());
                    CephEntity cephEntity = getCephEntityCredentials(cephOutputLocation);
                    uploadToCeph(path, requestId, workflow, scriptVariable.getName(), cephEntity, abstractDataFrame, cephOutputLocation);
                    processCephIngestionSuccess(requestId, workflow.getId(), scriptVariable.getName(), workflow.getName(), cephEntity, cephOutputLocation);
                } catch (Exception e) {
                    String errorMessage = String.format("Error In CephIngestion for DataFrame %s : ErrorMessage : %s.",
                            scriptVariable.getName(), e.getMessage());
                    processCephIngestionFailure(requestId, workflow.getId(), scriptVariable.getName(), errorMessage, errorMessages, cephOutputLocation);
                }
            }
        }
        return errorMessages;
    }

    private List<CephOutputLocation> getCephOutputLocations(ScriptVariable scriptVariable) {
        return scriptVariable.getOutputLocationDetailsList().stream()
                .filter(outputLocation -> outputLocation instanceof CephOutputLocation)
                .map(outputLocation -> (CephOutputLocation) outputLocation).collect(toList());
    }

    private String getHDFSPath(Long requestId, String path, String workflowName, String dataFrameName) {
        return HDFS_CLUSTER_PREFIX + HADOOP_CLUSTER + miscConfig.getCephBaseHDFSPath() + slash
                + workflowName + slash + dataFrameName + (Objects.isNull(path) ? "" : slash + path)
                + slash + Constants.REFRESH_ID + equal + requestId.toString();
    }

    private void uploadToCeph(String path, Long requestId, Workflow workflow, String dataFrameName, CephEntity cephEntity,
                             AbstractDataFrame abstractDataFrame, CephOutputLocation cephOutputLocation) throws IOException, InterruptedException, CephIngestionException {
        String hadoopUser = hadoopConfig.getUser();
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUser);
        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            try {
                List<String> files = hdfsUtils.getAllFilesUnderDirectory(new Path(path));
                uploadSingleFileToCeph(requestId, workflow, dataFrameName, files, cephEntity, abstractDataFrame, cephOutputLocation);
                uploadPartitionsFileToCeph(requestId, workflow.getName(), dataFrameName, path, files, cephEntity, cephOutputLocation);
                return null;
            } catch (Exception e) {
                e.printStackTrace();
                throw new CephIngestionException(e.getMessage(), e);
            } finally {
                deleteTempDir(requestId);
            }
        });
    }

    private void uploadSingleFileToCeph(Long requestId, Workflow workflow, String dataFrameName, List<String> files, CephEntity cephEntity,
                                       AbstractDataFrame abstractDataFrame, CephOutputLocation cephOutputLocation) throws InterruptedException, IOException, HDFSUtilsException {
        if (cephOutputLocation.isMerged()) {
            boolean isMerged = false;
            String destinationDirectory = miscConfig.getCephBaseHDFSPath() + slash + "merged_output" + slash
                    + Constants.REFRESH_ID + equal + requestId.toString();
            hdfsUtils.deleteIfExist(destinationDirectory);
            try {
                String fileName = getCephFileName(files.get(0));
                String partitionHeader = files.get(0);
                if (files.size() != 1) {
                    isMerged = true;
                    fileName = Constants.REFRESH_ID + equal + requestId.toString() + CSV_EXTENSION;
                    files = mergeFiles(workflow.getWorkflowMeta().getHiveQueue(), destinationDirectory, files, abstractDataFrame);
                }
                String cephKey = generateCephKey(fileName, requestId, workflow.getName(), dataFrameName, cephOutputLocation.getPath());
                insertHeaderInMergedFile(files.get(0), partitionHeader, cephOutputLocation);
                processUpload(cephKey, files.get(0), cephOutputLocation.getBucket(), cephEntity, requestId);
            } finally {
                if (isMerged)
                    hdfsUtils.cleanFilesUnderDirectory(new Path(destinationDirectory));
            }
        }
    }

    private List<String> mergeFiles(String hiveQueue, String destinationDirectory, List<String> files, AbstractDataFrame abstractDataFrame) throws InterruptedException, IOException {
        mergeDataByMapReduce(hiveQueue, destinationDirectory, files, abstractDataFrame);
        List<String> mergedFiles = hdfsUtils.getAllFilesUnderDirectory(new Path(destinationDirectory));
        if (mergedFiles.size() != 1) {
            throw new CephIngestionException("No File or more than one file found in merged directory for Ceph upload.");
        }
        return mergedFiles;
    }

    private void mergeDataByMapReduce(String hiveQueue, String destinationDirectory, List<String> files,
                                      AbstractDataFrame abstractDataFrame) throws InterruptedException, IOException {
        String separator = (!Objects.isNull(abstractDataFrame) && !Objects.isNull(abstractDataFrame.getSeparator()))
                ? abstractDataFrame.getSeparator() : comma;
        String[] mrArgs = {destinationDirectory, separator};

        Configuration configuration = new Configuration();
        configuration.set(MAPREDUCE_QUEUENAME_PROPERTY, hiveQueue);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopConfig.getUser());

        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            PartitionFileMergingDriver driver = new PartitionFileMergingDriver(files);
            try {
                int exitCode = ToolRunner.run(configuration, driver, mrArgs);
                if (exitCode != 0)
                    throw new CephIngestionException("Not able to merge Data for Ceph upload.");
            } catch (Exception e) {
                throw new CephIngestionException("Not able to merge Data for Ceph upload. Error: " + e.getMessage());
            }
            return null;
        });
    }

    private void uploadPartitionsFileToCeph(Long requestId, String workflowName, String dataFrameName, String path, List<String> files,
                                           CephEntity cephEntity, CephOutputLocation cephOutputLocation) throws IOException {
        if (!cephOutputLocation.isMerged()) {
            insertHeaderInPartitionsFile(path, cephOutputLocation);
            for (String file : files) {
                String fileName = getCephFileName(file);
                String cephKey = generateCephKey(fileName, requestId, workflowName, dataFrameName, cephOutputLocation.getPath());
                processUpload(cephKey, file, cephOutputLocation.getBucket(), cephEntity, requestId);
            }
        }
    }

    private String getCephFileName(String file) {
        String fileNameFromRefreshId = file.substring(file.indexOf(Constants.REFRESH_ID), file.lastIndexOf("/"));
        return fileNameFromRefreshId.replace(slash, "&") + CSV_EXTENSION;
    }

    private void processUpload(String cephKey, String file, String bucket, CephEntity cephEntity, Long requestId) throws IOException {
        InputStream inputStream = openInputStream(new Path(file));
        OutputStream outputStream = new MultipartUploadOutputStream(cephKey, miscConfig.getSaltKey(), bucket, cephEntity, requestId);
        copyStream(inputStream, outputStream);
        inputStream.close();
        outputStream.close();
    }

    private CephEntity getCephEntityCredentials(CephOutputLocation cephOutputLocation) throws IOException, DSPCoreException {
        String clientAlias = cephOutputLocation.getClientAlias();
        ExternalCredentials externalCredentials = externalCredentialsActor.getCredentials(clientAlias);
        return objectMapper.readValue(externalCredentials.getDetails(), CephEntity.class);
    }

    private String generateCephKey(String fileName, Long requestId, String workflowName, String dataFrameName, String userCephPath) {
        return userCephPath + slash + workflowName + slash + Constants.REFRESH_ID + equal
                + requestId.toString() + slash + dataFrameName + slash + fileName;
    }

    private InputStream openInputStream(Path inputFilePath) throws IOException {
        FileSystem inputFs = inputFilePath.getFileSystem(new Configuration());
        return inputFs.open(inputFilePath);
    }

    protected void copyStream(InputStream inputStream, OutputStream outputStream) throws IOException {
        int len;
        byte[] buffer = new byte[BUFFER_SIZE];
        while ((len = inputStream.read(buffer)) > 0) {
            outputStream.write(buffer, 0, len);
        }
    }

    private void processCephIngestionFailure(Long requestId, Long workflowId, String dataFrameName,
                                            String errorMessage, List<String> errorMessages, CephOutputLocation cephOutputLocation) {
        eventAuditUtil.createCephIngestionErrorEvent(requestId, workflowId, dataFrameName, errorMessage, cephOutputLocation);
        errorMessages.add(errorMessage);
    }

    private void processCephIngestionSuccess(Long requestId, Long workflowId, String dataFrameName, String workflowName,
                                            CephEntity cephEntity, CephOutputLocation cephOutputLocation) {
        List<URL> urls = AmazonS3Utils.getCephUrls(miscConfig.getSaltKey(), requestId, workflowName, dataFrameName,
                cephEntity, cephOutputLocation);
        eventAuditUtil.createCephIngestionEndInfoEvent(requestId, workflowId, dataFrameName, urls);
        printUrls(urls);
    }

    private void printUrls(List<URL> urls) {
        log.info("Ceph Links:");
        urls.forEach(url -> log.info(url.toString()));
    }

    private void insertHeaderInPartitionsFile(String path, CephOutputLocation cephOutputLocation) throws IOException {
        if (cephOutputLocation.getColumnMapping() != null && !cephOutputLocation.getColumnMapping().isEmpty()) {
            String headerRow = getNonPartitionHeaders(cephOutputLocation) + newLine;
            insertHeaderInAllFilesUnderDirectory(path, headerRow);
        }
    }

    private void insertHeaderInMergedFile(String file, String partitionHeader, CephOutputLocation cephOutputLocation) throws IOException {
        if (cephOutputLocation.getColumnMapping() != null && !cephOutputLocation.getColumnMapping().isEmpty()) {
            StringBuilder header = new StringBuilder();
            header.append(getNonPartitionHeaders(cephOutputLocation));
            header.append(comma);
            header.append(getPartitionHeaders(partitionHeader));
            header.append(newLine);
            insertHeaderInFile(header.toString(), file);
        }
    }

    private String getNonPartitionHeaders(CephOutputLocation cephOutputLocation) {
        List<String> headers = new ArrayList<>();
        cephOutputLocation.getColumnMapping().forEach((columnName, dataType) -> headers.add(columnName));
        return String.join(comma, headers);
    }

    private String getPartitionHeaders(String file) {
        StringBuilder header = new StringBuilder();
        List<String> partitions = Arrays.asList(file.substring(file.indexOf(Constants.REFRESH_ID), file.lastIndexOf(slash)).split("/"));
        Lists.reverse(partitions).forEach(partition -> {
            header.append(comma);
            header.append(partition, 0, partition.lastIndexOf(equal));
        });
        header.deleteCharAt(0);
        return header.toString();
    }

    private void insertHeaderInAllFilesUnderDirectory(String path, String headers) throws IOException {
        List<String> files = hdfsUtils.getAllFilesUnderDirectory(new Path(path));
        for (String file : files)
            insertHeaderInFile(headers, file);
    }

    private void insertHeaderInFile(String header, String file) throws IOException {
        String parentDir = file.substring(0, file.lastIndexOf(slash));
        String headerFile = parentDir + slash + "headers" + CSV_EXTENSION;
        hdfsUtils.writeToFile(header, headerFile);
        hdfsUtils.concatFiles(new Path(headerFile), new Path[]{new Path(file)});
        hdfsUtils.rename(headerFile, file);
    }

    private void deleteTempDir(Long requestId) throws IOException {
        String[] backupDirs = new Configuration().get("fs.s3.buffer.dir").split(comma);
        File dir = new File(backupDirs[0] + slash + Constants.REFRESH_ID + equal + requestId);
        FileUtils.deleteDirectory(dir);
    }


}
