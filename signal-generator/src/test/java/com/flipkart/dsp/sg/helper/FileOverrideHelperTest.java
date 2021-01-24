package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.config.HadoopConfig;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.flipkart.dsp.entities.sg.core.*;
import com.flipkart.dsp.entities.workflow.*;
import com.flipkart.dsp.models.CsvFormat;
import com.flipkart.dsp.models.ExternalCredentials;
import com.flipkart.dsp.models.externalentities.FTPEntity;
import com.flipkart.dsp.models.overrides.CSVDataframeOverride;
import com.flipkart.dsp.models.overrides.FTPDataframeOverride;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.sg.SignalDataType;
import com.flipkart.dsp.sg.exceptions.HDFSDataLoadException;
import com.flipkart.dsp.sg.jobs.PartitioningFileDriver;
import com.flipkart.dsp.sg.override.FileOverrideManager;
import com.flipkart.dsp.sg.utils.EventAuditUtil;
import com.flipkart.dsp.utils.Decryption;
import com.flipkart.dsp.utils.HdfsUtils;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.*;

import static com.flipkart.dsp.utils.Constants.PRODUCTION_HIVE_QUEUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyNew;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileOverrideHelper.class, PartitioningFileDriver.class, IOUtils.class, CSVParser.class, CSVRecord.class, Decryption.class, ToolRunner.class})
public class FileOverrideHelperTest {
    private Long requestId = 1L;
    private Long workflowId = 1L;
    private Long dataFrameId = 1L;
    private String clientAlias = "client_alias";
    private String dataFrameName = "testDataFrame";
    private String workflowName = "workflowName";
    private String hdfsPath = "hdfsPath";

    private CSVRecord csvRecord;
    private FTPEntity ftpEntity;
    private ExternalCredentials externalCredentials;
    private FileOverrideManager fileOverrideManager;
    @Mock
    private DataFrameOverrideAudit dataframeOverrideAudit;
    private List<CSVRecord> csvRecords = new ArrayList<>();
    private Set<DataFrame> dataFrames = new HashSet<>();
    private List<String> partitions = new ArrayList<>();
    private FileOverrideHelper fileOverrideHelper;
    private LinkedHashMap<String, SignalDataType> columnMapping = new LinkedHashMap<>();

    @Mock
    private Workflow workflow;
    @Mock
    private CSVParser csvParser;
    @Mock
    private HdfsUtils hdfsUtils;
    @Mock
    private DataFrame dataFrame;
    @Mock
    private FileSystem fileSystem;
    @Mock
    private MiscConfig miscConfig;
    @Mock
    private WorkflowMeta workflowMeta;
    @Mock
    private HadoopConfig hadoopConfig;
    @Mock
    private PipelineStep pipelineStep;
    @Mock
    private FTPFileSystem ftpFileSystem;
    @Mock
    private EventAuditUtil eventAuditUtil;
    @Mock
    private DataFrameAudit dataFrameAudit;
    @Mock
    private WorkflowDetails workflowDetails;
    @Mock
    private FSDataOutputStream outputStream;
    @Mock
    private FSDataInputStream fsDataInputStream;
    @Mock
    private FTPDataframeOverride ftpDataframeOverride;
    @Mock
    private CSVDataframeOverride csvDataframeOverride;
    @Mock
    private PartitioningFileDriver partitioningFileDriver;
    @Mock
    private DataFrameOverrideHelper dataFrameOverrideHelper;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.mockStatic(PartitioningFileDriver.class);
        PowerMockito.mockStatic(IOUtils.class);
        PowerMockito.mockStatic(CSVParser.class);
        PowerMockito.mockStatic(CSVRecord.class);
        PowerMockito.mockStatic(Decryption.class);
        PowerMockito.mockStatic(ToolRunner.class);
        csvRecord = PowerMockito.mock(CSVRecord.class);


        csvRecords.add(csvRecord);
        dataFrames.add(dataFrame);
        columnMapping.put("column", SignalDataType.INTEGER);
        externalCredentials = ExternalCredentials.builder().clientAlias(clientAlias).details("details").build();
        fileOverrideHelper = spy(new FileOverrideHelper(hdfsUtils, fileSystem, miscConfig, hadoopConfig, ftpFileSystem, eventAuditUtil, dataFrameOverrideHelper));


        when(dataFrame.getId()).thenReturn(dataFrameId);
        when(dataFrame.getName()).thenReturn(dataFrameName);

        when(workflow.getId()).thenReturn(workflowId);
        when(workflow.getName()).thenReturn(workflowName);
        when(workflow.getDataFrames()).thenReturn(dataFrames);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);


        when(hadoopConfig.getBasePath()).thenReturn("base_path");
        when(hadoopConfig.getUser()).thenReturn("fk-ip-data-service");

        when(miscConfig.getSaltKey()).thenReturn("saltKey");
        when(miscConfig.getFtpBaseHDFSPath()).thenReturn("ftpBaseHDFSPath");

        when(csvDataframeOverride.getPath()).thenReturn("/local_csv_path/path.csv");
        when(csvDataframeOverride.getCsvFormat()).thenReturn(CsvFormat.Oracle);
        when(csvDataframeOverride.getColumnMapping()).thenReturn(columnMapping);

        when(ftpDataframeOverride.getPath()).thenReturn("/ftp_path/path.csv");
        when(ftpDataframeOverride.getColumnMapping()).thenReturn(columnMapping);

//        when(ftpDataframeOverride.getClientAlias()).thenReturn(clientAlias);
//        when(ftpDataframeOverride.getCsvFormat()).thenReturn(CsvFormat.Oracle);

        when(workflow.getWorkflowMeta()).thenReturn(workflowMeta);
        when(workflowMeta.getHiveQueue()).thenReturn(PRODUCTION_HIVE_QUEUE);
        when(dataFrameOverrideHelper.getDataframeValues(hdfsPath)).thenReturn(new LinkedHashSet<>());
        doNothing().when(dataFrameOverrideHelper).updateDataframeOverrideAudit(dataframeOverrideAudit, DataFrameOverrideState.SUCCEDED);
        when(dataFrameOverrideHelper.saveDataframeOverrideAudit(any(), any(), any(), any(), any(), any())).thenReturn(dataframeOverrideAudit);
        when(dataFrameOverrideHelper.saveDataFrameAudit(any(), any(), any(), any(), any())).thenReturn(dataFrameAudit);

        IOUtils.copyBytes(any(), any(), any(), anyBoolean());
        when(fileSystem.create(any())).thenReturn(outputStream);
        when(ftpFileSystem.open(any(), anyInt())).thenReturn(fsDataInputStream);
        when(hdfsUtils.loadFromHDFS(anyString(), any())).thenReturn("dataFromHDFS");
        PowerMockito.whenNew(CSVParser.class).withAnyArguments().thenReturn(csvParser);
        PowerMockito.when(Decryption.decrypt(anyString(), anyString())).thenReturn("password");
        PowerMockito.when(pipelineStep.getPartitions()).thenReturn(partitions);
        doNothing().when(eventAuditUtil).createCSVOverrideEndDebugEvent(requestId, workflowId, workflowName, dataFrameName);
    }

    @Test
    public void testGetHDFSPathCase1() throws Exception {
        String expected = fileOverrideHelper.getHDFSPath(requestId, workflow, dataFrameName, csvDataframeOverride, DataFrameOverrideType.CSV);
        assertEquals(expected, "/local_csv_path/path.csv");
        verify(csvDataframeOverride).getPath();
    }

    @Test
    public void testGetHDFSPathCase2() throws Exception {
        FTPEntity ftpEntity = FTPEntity.builder().password("password").user("user").host("host").build();
        when(dataFrameOverrideHelper.getFTPClientCredentials(ftpDataframeOverride)).thenReturn(ftpEntity);

        String expected = fileOverrideHelper.getHDFSPath(requestId, workflow, dataFrameName, ftpDataframeOverride, DataFrameOverrideType.FTP);
        assertEquals(expected, "ftpBaseHDFSPath/workflowName/refresh_id=1/testDataFrame/path.csv");
        verify(workflow).getName();
        verify(hadoopConfig).getUser();
        verify(ftpDataframeOverride).getPath();
        verify(dataFrameOverrideHelper).getFTPClientCredentials(ftpDataframeOverride);
        verify(miscConfig).getSaltKey();
        verify(ftpFileSystem).open(any(), anyInt());
        verify(miscConfig).getFtpBaseHDFSPath();
        verify(fileSystem, times(1)).create(any());
    }

    @Test
    public void testGetColumnMapping() {
        assertEquals(fileOverrideHelper.getColumnMapping(ftpDataframeOverride, DataFrameOverrideType.FTP), columnMapping);
        verify(ftpDataframeOverride, times(1)).getColumnMapping();
        assertEquals(fileOverrideHelper.getColumnMapping(csvDataframeOverride, DataFrameOverrideType.CSV), columnMapping);
        verify(csvDataframeOverride, times(1)).getColumnMapping();
    }

    @Test
    public void testGetPartitionColumnsInHeader() {
        String partition = "column1";
        String[] headers = new String[1];
        headers[0] = partition;
        List<String> partitions = new ArrayList<>();
        partitions.add(partition);
        List<String> expected = fileOverrideHelper.getPartitionColumnsInHeader(headers, partitions);
        assertEquals(expected.get(0), partition);
    }

    @Test
    public void testProcessNonPartitionedCSVTestFailure() throws Exception {
        when(dataFrame.getName()).thenReturn(workflowName);
        boolean isException = false;
        try {
            fileOverrideHelper.processNonPartitionedCSV(requestId, dataFrameName, hdfsPath, DataFrameOverrideType.CSV, workflowDetails, partitions);
        } catch (IllegalArgumentException e) {
            isException = true;
            assertEquals(e.getMessage(), "DataFrame config missing for dataFrame : " + dataFrameName);
        }

        assertTrue(isException);
        verify(workflowDetails).getWorkflow();
        verify(workflow).getDataFrames();
        verify(dataFrame).getName();
    }

    @Test
    public void testProcessNonPartitionedCSVTestSuccess() throws Exception {
        when(dataframeOverrideAudit.getId()).thenReturn(1L);
        when(hdfsUtils.getFolderSize(hdfsPath)).thenThrow(new IOException(""));

        fileOverrideHelper.processNonPartitionedCSV(requestId, dataFrameName, hdfsPath, DataFrameOverrideType.CSV, workflowDetails, partitions);
        verify(workflowDetails).getWorkflow();
        verify(workflow).getDataFrames();
        verify(dataFrame).getName();
        verify(dataFrameOverrideHelper).getDataframeValues(hdfsPath);
        verify(workflow, times(2)).getId();
        verify(workflow).getName();
        verify(dataFrameOverrideHelper).saveDataframeOverrideAudit(any(), any(), any(), any(), any(), any());
        verify(dataFrameOverrideHelper).updateDataframeOverrideAudit(dataframeOverrideAudit, DataFrameOverrideState.SUCCEDED);
        verify(hdfsUtils).getFolderSize(hdfsPath);
        verify(eventAuditUtil).createCSVOverrideEndDebugEvent(requestId, workflowId, workflowName, dataFrameName);
        verify(dataframeOverrideAudit).getId();
        verify(dataFrameOverrideHelper).saveDataFrameAudit(any(), any(), any(), any(), any());
    }

    @Test
    public void testMoveDataFrameInHDFSCase1Success() throws Exception {
        partitions.add("column1");
        partitions.add("column2");

        when(hdfsUtils.getFolderSize(hdfsPath)).thenReturn(1L);
        doReturn(columnMapping).when(fileOverrideHelper).getColumnMapping(csvDataframeOverride, DataFrameOverrideType.CSV);
        when(hadoopConfig.getCsvFileSizeThreshold()).thenReturn(10.0);
        PowerMockito.when(pipelineStep.getPartitions()).thenReturn(partitions);
        when(csvParser.getRecords()).thenReturn(csvRecords);
        when(csvRecord.get("column1")).thenReturn("column1");
        when(csvRecord.get("column2")).thenThrow(new IllegalArgumentException());

        DataFrameAudit expected = fileOverrideHelper.moveDataFrameInHDFS(requestId, dataFrameName, hdfsPath, workflowDetails, csvDataframeOverride, DataFrameOverrideType.CSV, partitions);
        assertEquals(expected, dataFrameAudit);

        verify(workflowDetails).getWorkflow();
        verify(workflow).getDataFrames();
        verify(dataFrame, times(4)).getName();
        verify(workflow, times(2)).getId();
        verify(dataFrame, times(2)).getId();
        verify(dataFrameOverrideHelper).saveDataframeOverrideAudit(requestId, workflowId, dataFrameId,
                "_csv_", hdfsPath, DataFrameOverrideType.CSV);
        verify(hdfsUtils).getFolderSize(hdfsPath);
        verify(fileOverrideHelper).getColumnMapping(csvDataframeOverride, DataFrameOverrideType.CSV);
        verify(csvDataframeOverride).getCsvFormat();
        verify(hadoopConfig).getCsvFileSizeThreshold();
        verify(hdfsUtils).loadFromHDFS(anyString(), any());
        verify(hadoopConfig).getBasePath();
        verify(dataFrameOverrideHelper).updateDataframeOverrideAudit(dataframeOverrideAudit, DataFrameOverrideState.SUCCEDED);
        verify(workflow, times(2)).getId();
        verify(csvParser).getRecords();
        verify(csvRecord).get("column1");
        verify(csvRecord).get("column2");
        verify(eventAuditUtil).createCSVOverrideEndDebugEvent(requestId, workflowId, workflowName, dataFrameName);
    }

    @Test
    public void testMoveDataFrameInHDFSSuccessCase2Success() throws Exception {
        List<String> files = new ArrayList<>();
        files.add("file");

        when(hdfsUtils.getFolderSize(hdfsPath)).thenReturn(20L);
        doReturn(columnMapping).when(fileOverrideHelper).getColumnMapping(csvDataframeOverride, DataFrameOverrideType.CSV);
        when(hadoopConfig.getCsvFileSizeThreshold()).thenReturn(10.0);
        PowerMockito.whenNew(PartitioningFileDriver.class).withAnyArguments().thenReturn(partitioningFileDriver);
        PowerMockito.when(ToolRunner.run(any(), any(), any())).thenReturn(0);
        when(hdfsUtils.getFileNamesUnderDirectory(any())).thenReturn(files);
        when(dataFrameOverrideHelper.getDataFrameKeys(any())).thenReturn(new ArrayList<>());
        when(dataFrameOverrideHelper.getDataframeValues(any())).thenReturn(new LinkedHashSet<>());

        DataFrameAudit expected = fileOverrideHelper.moveDataFrameInHDFS(requestId, dataFrameName, hdfsPath, workflowDetails, csvDataframeOverride, DataFrameOverrideType.CSV, partitions);
        assertEquals(expected, dataFrameAudit);
        verify(workflowDetails).getWorkflow();
        verify(workflow).getDataFrames();
        verify(dataFrame, times(7)).getName();
        verify(workflow, times(2)).getId();
        verify(dataFrame).getId();
        verify(dataFrameOverrideHelper).saveDataframeOverrideAudit(requestId, workflowId, dataFrameId,
                "_csv_", hdfsPath, DataFrameOverrideType.CSV);
        verify(hdfsUtils).getFolderSize(hdfsPath);
        verify(fileOverrideHelper).getColumnMapping(csvDataframeOverride, DataFrameOverrideType.CSV);
        verify(csvDataframeOverride).getCsvFormat();
        verify(hadoopConfig).getCsvFileSizeThreshold();
        verify(hadoopConfig).getBasePath();
        verify(workflow, times(2)).getId();
        verifyNew(PartitioningFileDriver.class).withArguments(any(), any(), any(), any());
        verifyStatic(ToolRunner.class);
        ToolRunner.run(any(), any(), any());
        verify(dataFrameOverrideHelper).getDataFrameKeys(any());
        verify(dataFrameOverrideHelper).getDataframeValues(any());
        verify(dataFrameOverrideHelper).updateDataframeOverrideAudit(dataframeOverrideAudit, DataFrameOverrideState.SUCCEDED);
        verify(eventAuditUtil).createCSVOverrideEndDebugEvent(requestId, workflowId, workflowName, dataFrameName);
    }

    @Test
    public void testMoveDataFrameInHDFSCase1Failure() throws Exception {
        when(hdfsUtils.loadFromHDFS(anyString(), any())).thenThrow(new RuntimeException());
        when(hdfsUtils.getFolderSize(hdfsPath)).thenReturn(1L);
        doReturn(columnMapping).when(fileOverrideHelper).getColumnMapping(csvDataframeOverride, DataFrameOverrideType.CSV);
        when(hadoopConfig.getCsvFileSizeThreshold()).thenReturn(10.0);

        boolean isException = false;
        try {
            fileOverrideHelper.moveDataFrameInHDFS(requestId, dataFrameName, hdfsPath, workflowDetails, csvDataframeOverride, DataFrameOverrideType.CSV, partitions);
        } catch (HDFSDataLoadException e) {
            isException = true;
            assertTrue(e.getMessage().contains("No real dataFrame generated for DataFrame : "));
        }

        assertTrue(isException);
        verify(workflowDetails).getWorkflow();
        verify(workflow).getDataFrames();
        verify(dataFrame, times(4)).getName();
        verify(workflow, times(2)).getId();
        verify(dataFrame).getId();
        verify(dataFrameOverrideHelper).saveDataframeOverrideAudit(requestId, workflowId, dataFrameId,
                "_csv_", hdfsPath, DataFrameOverrideType.CSV);
        verify(hdfsUtils).getFolderSize(hdfsPath);
        verify(fileOverrideHelper).getColumnMapping(csvDataframeOverride, DataFrameOverrideType.CSV);
        verify(csvDataframeOverride).getCsvFormat();
        verify(hadoopConfig).getCsvFileSizeThreshold();
        verify(hdfsUtils).loadFromHDFS(anyString(), any());
        verify(dataFrameOverrideHelper).updateDataframeOverrideAudit(dataframeOverrideAudit, DataFrameOverrideState.FAILED);
        verify(workflow).getName();
        verify(eventAuditUtil).createCSVOverrideErrorEvent(any(), any(), any(), any(), any());

    }

    @Test
    public void testMoveDataFrameInHDFSSuccessCase2Failure() throws Exception {
        when(hdfsUtils.getFolderSize(hdfsPath)).thenReturn(20L);
        doReturn(columnMapping).when(fileOverrideHelper).getColumnMapping(csvDataframeOverride, DataFrameOverrideType.CSV);
        when(hadoopConfig.getCsvFileSizeThreshold()).thenReturn(10.0);
        PowerMockito.whenNew(PartitioningFileDriver.class).withAnyArguments().thenReturn(partitioningFileDriver);
        PowerMockito.when(ToolRunner.run(any(), any(), any())).thenThrow(new Exception());

        boolean isException = false;
        try {
            fileOverrideHelper.moveDataFrameInHDFS(requestId, dataFrameName, hdfsPath, workflowDetails, csvDataframeOverride, DataFrameOverrideType.CSV, partitions);
        } catch (HDFSDataLoadException e) {
            isException = true;
            assertTrue(e.getMessage().contains("No real dataFrame generated for DataFrame : "));
        }

        assertTrue(isException);
        verify(workflowDetails).getWorkflow();
        verify(workflow).getDataFrames();
        verify(dataFrame, times(7)).getName();
        verify(workflow, times(2)).getId();
        verify(dataFrame).getId();
        verify(dataFrameOverrideHelper).saveDataframeOverrideAudit(requestId, workflowId, dataFrameId,
                "_csv_", hdfsPath, DataFrameOverrideType.CSV);
        verify(hdfsUtils).getFolderSize(hdfsPath);
        verify(fileOverrideHelper).getColumnMapping(csvDataframeOverride, DataFrameOverrideType.CSV);
        verify(csvDataframeOverride).getCsvFormat();
        verify(hadoopConfig).getCsvFileSizeThreshold();
        verify(hadoopConfig).getBasePath();
        verify(workflow, times(2)).getId();
        verifyNew(PartitioningFileDriver.class).withArguments(any(), any(), any(), any());
        verifyStatic(ToolRunner.class);
        ToolRunner.run(any(), any(), any());
        verify(dataFrameOverrideHelper).updateDataframeOverrideAudit(dataframeOverrideAudit, DataFrameOverrideState.FAILED);
        verify(eventAuditUtil).createCSVOverrideErrorEvent(any(), any(), any(), any(), any());
    }

    @Test
    public void testMoveDataFrameInHDFSSuccessCase3Failure() throws Exception {
        when(hdfsUtils.getFolderSize(hdfsPath)).thenReturn(20L);
        doReturn(columnMapping).when(fileOverrideHelper).getColumnMapping(csvDataframeOverride, DataFrameOverrideType.CSV);
        when(hadoopConfig.getCsvFileSizeThreshold()).thenReturn(10.0);
        PowerMockito.whenNew(PartitioningFileDriver.class).withAnyArguments().thenReturn(partitioningFileDriver);
        PowerMockito.when(ToolRunner.run(any(), any(), any())).thenReturn(1);

        boolean isException = false;
        try {
            fileOverrideHelper.moveDataFrameInHDFS(requestId, dataFrameName, hdfsPath, workflowDetails, csvDataframeOverride, DataFrameOverrideType.CSV, partitions);
        } catch (HDFSDataLoadException e) {
            isException = true;
            assertTrue(e.getMessage().contains("No real dataFrame generated for DataFrame : "));
        }

        assertTrue(isException);
        verify(workflowDetails).getWorkflow();
        verify(workflow).getDataFrames();
        verify(dataFrame, times(8)).getName();
        verify(workflow, times(2)).getId();
        verify(dataFrame).getId();
        verify(dataFrameOverrideHelper).saveDataframeOverrideAudit(requestId, workflowId, dataFrameId,
                "_csv_", hdfsPath, DataFrameOverrideType.CSV);
        verify(hdfsUtils).getFolderSize(hdfsPath);
        verify(fileOverrideHelper).getColumnMapping(csvDataframeOverride, DataFrameOverrideType.CSV);
        verify(csvDataframeOverride).getCsvFormat();
        verify(hadoopConfig).getCsvFileSizeThreshold();
        verify(hadoopConfig).getBasePath();
        verify(workflow, times(2)).getId();
        verifyNew(PartitioningFileDriver.class).withArguments(any(), any(), any(), any());
        verifyStatic(ToolRunner.class);
        ToolRunner.run(any(), any(), any());
        verify(dataFrameOverrideHelper).updateDataframeOverrideAudit(dataframeOverrideAudit, DataFrameOverrideState.FAILED);
        verify(eventAuditUtil).createCSVOverrideErrorEvent(any(), any(), any(), any(), any());
    }

}
