package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.config.HadoopConfig;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.entities.sg.core.*;
import com.flipkart.dsp.entities.sg.dto.DataFrameKey;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.models.externalentities.FTPEntity;
import com.flipkart.dsp.models.overrides.*;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.SignalDataType;
import com.flipkart.dsp.sg.exceptions.HDFSDataLoadException;
import com.flipkart.dsp.sg.exceptions.SGJobException;
import com.flipkart.dsp.sg.jobs.PartitioningFileDriver;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import com.flipkart.dsp.sg.utils.StrictHashMap;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.utils.*;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.assertj.core.util.Lists;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.*;

import static com.flipkart.dsp.utils.Constants.*;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class FileOverrideHelper {
    private final HdfsUtils hdfsUtils;
    private final FileSystem fileSystem;
    private final MiscConfig miscConfig;
    private final HadoopConfig hadoopConfig;
    private final FTPFileSystem ftpFileSystem;
    private final EventAuditUtil eventAuditUtil;
    private final DataFrameOverrideHelper dataFrameOverrideHelper;

    private static final String SEARCH_PATTERN = "/*-r-*";
    private static final String SPLIT_PATTERN = "-r-";
    private static final String EXCLUDE_PART = "part";

    public String getHDFSPath(Long requestId, Workflow workflow, String dataFrameName, DataframeOverride dataframeOverride,
                              DataFrameOverrideType overrideType) throws IOException, InterruptedException, URISyntaxException {
        if (overrideType.toString().equalsIgnoreCase(DataFrameOverrideType.FTP.toString())) {
            return copyDataToHDFS(requestId, workflow.getName(), dataFrameName, (FTPDataframeOverride) dataframeOverride);
        } else {
            return ((CSVDataframeOverride) dataframeOverride).getPath();
        }
    }

    private String copyDataToHDFS(Long requestId, String workflowName, String dataFrameName,
                                  FTPDataframeOverride ftpDataframeOverride) throws IOException, InterruptedException, URISyntaxException {
        String hadoopUser = hadoopConfig.getUser();
        String src = ftpDataframeOverride.getPath();

        FTPEntity ftpEntity = dataFrameOverrideHelper.getFTPClientCredentials(ftpDataframeOverride);
        String url = ftp + colon + slash + slash + ftpEntity.getUser() + colon
                + Decryption.decrypt(ftpEntity.getPassword(), miscConfig.getSaltKey()) + "@" + ftpEntity.getHost();

        Configuration configuration = new Configuration();
        configuration.set(HADOOP_USER_PROPERTY, hadoopUser);

        ftpFileSystem.setConf(configuration);
        ftpFileSystem.initialize(new URI(url), configuration);
        FSDataInputStream inputSteam = ftpFileSystem.open(new Path(src), 1000);

        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUser);
        String path = miscConfig.getFtpBaseHDFSPath() + slash + workflowName + equal
                + requestId + slash + dataFrameName + slash + src.substring(src.lastIndexOf(slash) + 1);

        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            OutputStream outputStream = fileSystem.create(new Path(path));
            IOUtils.copyBytes(inputSteam, outputStream, configuration, true);
            return null;
        });
        return path;
    }

    public LinkedHashMap<String, SignalDataType> getColumnMapping(DataframeOverride dataframeOverride, DataFrameOverrideType overrideType) {
        return overrideType.toString().equalsIgnoreCase(DataFrameOverrideType.FTP.toString()) ?
                ((FTPDataframeOverride) dataframeOverride).getColumnMapping() : ((CSVDataframeOverride) dataframeOverride).getColumnMapping();
    }

    public List<String> getPartitionColumnsInHeader(String[] headers, List<String> partitions) {
        List<String> partitionList = new ArrayList<>();
        for (String header : headers) {
            if (partitions.contains(header)) partitionList.add(header);
        }
        return partitionList;
    }

    public DataFrameAudit processNonPartitionedCSV(Long requestId, String dataFrameName, String hdfsPath,
                                                   DataFrameOverrideType overrideType, WorkflowDetails workflowDetails,
                                                   List<String> partitions) throws Exception {
        Workflow workflow = workflowDetails.getWorkflow();
        DataFrame dataFrame = getDataFrameByName(workflow, dataFrameName);
        DataFrameOverrideAudit dataframeOverrideAudit = dataFrameOverrideHelper.saveDataframeOverrideAudit(requestId,
                workflow.getId(), dataFrame.getId(), "_csv_", hdfsPath, overrideType);
        StrictHashMap<List<DataFrameKey>, Set<String>> auditEntry = new StrictHashMap<>(); // if partition is not needed, set the csv path in data-frame audit and return
        auditEntry.put(new ArrayList<>(), dataFrameOverrideHelper.getDataframeValues(hdfsPath));
        SGUseCasePayload sgUseCasePayload = new SGUseCasePayload(requestId, dataFrameName, dataFrameOverrideHelper.getColumnMetadata(partitions), auditEntry);
        Long dataFrameSize = getDataFrameSize(hdfsPath);
        eventAuditUtil.createCSVOverrideEndDebugEvent(requestId, workflow.getId(), workflow.getName(), dataFrameName);
        dataframeOverrideAudit.setOutputMetadata(hdfsPath);
        dataFrameOverrideHelper.updateDataframeOverrideAudit(dataframeOverrideAudit, DataFrameOverrideState.SUCCEDED);
        return dataFrameOverrideHelper.saveDataFrameAudit(dataFrameSize, dataframeOverrideAudit.getId(), partitions, dataFrame, sgUseCasePayload);
    }

    private DataFrame getDataFrameByName(Workflow workflow, String dataFrameName) {
        for (DataFrame dataFrame : workflow.getDataFrames()) {
            if (dataFrame.getName().equals(dataFrameName)) {
                return dataFrame;
            }
        }
        throw new IllegalArgumentException("DataFrame config missing for dataFrame : " + dataFrameName);
    }

    private long getDataFrameSize(String path) {
        long fileSize = 0;
        try {
            fileSize = hdfsUtils.getFolderSize(path);
            log.info("Path; {} , CSV File Size: {}", path, StringUtils.TraditionalBinaryPrefix.long2String(fileSize, "", 1));
        } catch (IOException e) {
            log.warn("Failed to update DataFrame Size", e);
        }
        return fileSize;
    }

    public DataFrameAudit moveDataFrameInHDFS(Long requestId, String dataFrameName, String hdfsPath, WorkflowDetails workflowDetails,
                                              DataframeOverride dataframeOverride, DataFrameOverrideType overrideType,
                                              List<String> partitions) throws Exception {
        Workflow workflow = workflowDetails.getWorkflow();
        String hiveQueue = workflow.getWorkflowMeta().getHiveQueue();
        DataFrame dataFrame = getDataFrameByName(workflow, dataFrameName);
        DataFrameOverrideAudit dataframeOverrideAudit = dataFrameOverrideHelper.saveDataframeOverrideAudit(requestId, workflow.getId(),
                dataFrame.getId(), "_csv_", hdfsPath, overrideType);
        Long dataFrameSize = getDataFrameSize(hdfsPath);
        try {
            String[] headers = getHeaders(dataframeOverride, overrideType);
            List<String> headersList = Lists.newArrayList(headers);
            CSVFormat format = CSVFormat.valueOf(getCSVFormat(overrideType, dataframeOverride)).withHeader(headers);
            StrictHashMap<List<DataFrameKey>, Set<String>> auditEntry = generateDataFrame(dataFrameSize, hdfsPath, hiveQueue,
                    format, dataFrame, headersList, partitions, dataframeOverrideAudit);
            updateDataFrameOverrideAudit(hdfsPath, dataframeOverrideAudit);
            SGUseCasePayload sgUseCasePayload = new SGUseCasePayload(requestId, dataFrame.getName(), dataFrameOverrideHelper.getColumnMetadata(partitions), auditEntry);
            eventAuditUtil.createCSVOverrideEndDebugEvent(requestId, workflow.getId(), workflow.getName(), dataFrame.getName());
            return dataFrameOverrideHelper.saveDataFrameAudit(dataFrameSize, dataframeOverrideAudit.getId(), partitions, dataFrame, sgUseCasePayload);
        } catch (Exception e) {
            String errorMessage = "No real dataFrame generated for DataFrame : " + dataFrame.getName() + " by thread - " + Thread.currentThread().getName();
            eventAuditUtil.createCSVOverrideErrorEvent(requestId, workflow.getId(), workflow.getName(), dataFrame.getName(), errorMessage);
            throw new HDFSDataLoadException(errorMessage);
        }
    }

    private String[] getHeaders(DataframeOverride dataframeOverride, DataFrameOverrideType overrideType) {
        LinkedHashMap<String, SignalDataType> columnMapping = getColumnMapping(dataframeOverride, overrideType);
        return columnMapping.keySet().toArray(new String[0]);
    }

    private String getCSVFormat(DataFrameOverrideType overrideType, DataframeOverride dataframeOverride) {
        return overrideType.toString().equalsIgnoreCase(DataFrameOverrideType.FTP.toString()) ?
                ((FTPDataframeOverride) dataframeOverride).getCsvFormat().name() : ((CSVDataframeOverride) dataframeOverride).getCsvFormat().name();
    }

    private StrictHashMap<List<DataFrameKey>, Set<String>> generateDataFrame(Long dataFrameSize, String hdfsPath, String hiveQueue,
                                                                             CSVFormat format, DataFrame dataFrame, List<String> headersList,
                                                                             List<String> partitions, DataFrameOverrideAudit dataFrameOverrideAudit) throws Exception {
        if (dataFrameSize <= hadoopConfig.getCsvFileSizeThreshold()) {
            return inMemoryDataFrameGeneration(hdfsPath, format, dataFrame, headersList, partitions, dataFrameOverrideAudit);
        } else {
            return mapReduceDataFrameGeneration(hdfsPath, hiveQueue, format, dataFrame, headersList, partitions, dataFrameOverrideAudit);
        }
    }

    private StrictHashMap<List<DataFrameKey>, Set<String>> inMemoryDataFrameGeneration(String hdfsPath, CSVFormat format, DataFrame dataFrame, List<String> headersList,
                                                                                       List<String> partitions, DataFrameOverrideAudit dataFrameOverrideAudit) throws Exception {
        try {
            StrictHashMap<List<DataFrameKey>, Set<String>> auditEntry = new StrictHashMap<>();
            log.info("InMemory dataFrame generation from csv started for DataFrame: " + dataFrame.getName() + " by thread - " + Thread.currentThread().getName());
            Reader in = new StringReader(hdfsUtils.loadFromHDFS(hdfsPath, format.getRecordSeparator()));
            CSVParser parser = new CSVParser(in, format);
            Map<List<String>, List<String>> partitionToRowMap = partitionDataByPartitionKeys(parser.getRecords(),
                    partitions, headersList, dataFrame, format);
            for (List<String> partition : partitionToRowMap.keySet()) {
                String path = String.format("%s/%s", hadoopConfig.getBasePath(), UUID.randomUUID().toString());
                hdfsUtils.writeToFile(String.join(Constants.newLine, partitionToRowMap.get(partition)), path);
                auditEntry.put(dataFrameOverrideHelper.getDataFrameKeys(partition), dataFrameOverrideHelper.getDataframeValues(path));
            }
            return auditEntry;
        } catch (Exception e) {
            log.error("CSV DataFrame override InMemory Job failed", e);
            dataFrameOverrideHelper.updateDataframeOverrideAudit(dataFrameOverrideAudit, DataFrameOverrideState.FAILED);
            throw e;
        }
    }

    private Map<List<String>, List<String>> partitionDataByPartitionKeys(List<CSVRecord> records, List<String> partitions,
                                                                         List<String> headers, DataFrame dataFrame, CSVFormat format) {
        Map<List<String>, List<String>> partitionsToRowMap = new HashMap<>();
        for (CSVRecord record : records) {
            List<String> partitionValues = new ArrayList<>();
            for (String partition : partitions) {
                try {
                    partitionValues.add(record.get(partition));
                } catch (IllegalArgumentException e) {
                    log.debug(String.format("Partition column %s not part of dataFrame: %s", partition, dataFrame.getId()));
                }
            }

            List<String> csvRowData = new ArrayList<>();
            for (String column : headers) {
                if (!partitions.contains(column))
                    csvRowData.add(record.get(column));
            }
            List<String> partitionedData = partitionsToRowMap.getOrDefault(partitionValues, new ArrayList<>());
            partitionedData.add(String.join(String.valueOf(format.getDelimiter()), csvRowData));
            partitionsToRowMap.put(partitionValues, partitionedData);
        }
        return partitionsToRowMap;
    }

    private void updateDataFrameOverrideAudit(String hdfsPath, DataFrameOverrideAudit dataFrameOverrideAudit) {
        dataFrameOverrideAudit.setOutputMetadata(hdfsPath);
        dataFrameOverrideHelper.updateDataframeOverrideAudit(dataFrameOverrideAudit, DataFrameOverrideState.SUCCEDED);
    }

    private StrictHashMap<List<DataFrameKey>, Set<String>> mapReduceDataFrameGeneration(String hdfsPath, String hiveQueue, CSVFormat format, DataFrame dataFrame, List<String> headersList,
                                                                                        List<String> partitions, DataFrameOverrideAudit dataFrameOverrideAudit) throws Exception {
        try {
            log.info("DataFrameName: " + dataFrame.getName());
            StrictHashMap<List<DataFrameKey>, Set<String>> auditEntry = new StrictHashMap<>();
            log.info("MapReduce dataFrame generation from csv started for DataFrame: " + dataFrame.getName() + " by thread - " + Thread.currentThread().getName());
            String destinationDirectory = hadoopConfig.getBasePath() + "/dataframes/" + UUID.randomUUID().toString() + "/" + dataFrame.getName();
            Integer exitCode = partitionDataViaMapReduce(hdfsPath, hiveQueue, destinationDirectory, dataFrame.getName(), format, headersList, partitions);
            if (exitCode != 0)
                throw new HDFSDataLoadException("No real dataFrame generated for DataFrame : " + dataFrame.getName() + " by thread - " + Thread.currentThread().getName());
            updateAuditMap(hdfsUtils.getFileNamesUnderDirectory(new Path(destinationDirectory + SEARCH_PATTERN)), auditEntry);
            return auditEntry;
        } catch (IOException | InterruptedException | HDFSDataLoadException | IllegalArgumentException | SGJobException e) {
            log.error("CSV DataFrame override MR failed", e);
            dataFrameOverrideHelper.updateDataframeOverrideAudit(dataFrameOverrideAudit, DataFrameOverrideState.FAILED);
            throw e;
        }
    }

    private void updateAuditMap(List<String> outputFiles, StrictHashMap<List<DataFrameKey>, Set<String>> auditEntry) throws SGJobException {
        log.info("files: " + outputFiles);
        for (String file : outputFiles) {
            String[] pathArray = file.split(Constants.slash);
            String fileName = pathArray[pathArray.length - 1].split(SPLIT_PATTERN)[0];
            if (!fileName.equals(EXCLUDE_PART)) {
                List<String> partitionValues = Arrays.asList(fileName.split("#"));
                auditEntry.put(dataFrameOverrideHelper.getDataFrameKeys(partitionValues), dataFrameOverrideHelper.getDataframeValues(file));
            }
        }
    }

    private Integer partitionDataViaMapReduce(String sourcePath, String hiveQueue, String destinationDirectory, String dataFrameId,
                                              CSVFormat format, List<String> headersList, List<String> partitionList) throws IOException, InterruptedException {
        String hdfsUser = hadoopConfig.getUser();
        Configuration conf = new Configuration();
        conf.set(MAPREDUCE_QUEUENAME_PROPERTY, hiveQueue);
        String[] mrArgs = {sourcePath, destinationDirectory};
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hdfsUser);

        return ugi.doAs((PrivilegedExceptionAction<Integer>) () -> {
            PartitioningFileDriver driver = new PartitioningFileDriver("dataFrame", Character.toString(format.getDelimiter()), headersList, partitionList);
            try {
                return ToolRunner.run(conf, driver, mrArgs);
            } catch (Exception e) {
                log.error(e.getMessage());
                throw new HDFSDataLoadException("No real dataFrame generated for DataFrame : " + dataFrameId);
            }
        });
    }

}
