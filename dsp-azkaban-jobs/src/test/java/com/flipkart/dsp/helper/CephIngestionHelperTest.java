package com.flipkart.dsp.helper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.dsp.actors.ExternalCredentialsActor;
import com.flipkart.dsp.config.HadoopConfig;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.cephingestion.MultipartUploadOutputStream;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.entities.workflow.WorkflowMeta;
import com.flipkart.dsp.models.ExternalCredentials;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.externalentities.CephEntity;
import com.flipkart.dsp.models.outputVariable.CephOutputLocation;
import com.flipkart.dsp.models.outputVariable.OutputLocation;
import com.flipkart.dsp.models.sg.SignalDataType;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.utils.AmazonS3Utils;
import com.flipkart.dsp.utils.EventAuditUtil;
import com.flipkart.dsp.utils.HdfsUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static com.flipkart.dsp.utils.Constants.PRODUCTION_HIVE_QUEUE;
import static com.flipkart.dsp.utils.Constants.comma;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CephIngestionHelper.class, MultipartUploadOutputStream.class, Path.class, URL.class, AmazonS3Utils.class, ToolRunner.class})
public class CephIngestionHelperTest {


    @Mock private Path path3;
    @Mock private Workflow workflow;
    @Mock private HdfsUtils hdfsUtils;
    @Mock private CephEntity cephEntity;
    @Mock private FileSystem fileSystem;
    @Mock private MiscConfig miscConfig;
    @Mock private WorkflowMeta workflowMeta;
    @Mock private ObjectMapper objectMapper;
    @Mock private HadoopConfig hadoopConfig;
    @Mock private EventAuditUtil eventAuditUtil;
    @Mock private ScriptVariable scriptVariable;
    @Mock private FSDataInputStream inputStream;
    @Mock private WorkflowDetails workflowDetails;
    @Mock private CephOutputLocation cephOutputLocation;
    @Mock private AbstractDataFrame abstractDataFrame;
    @Mock private ExternalCredentials externalCredentials;
    @Mock private MultipartUploadOutputStream multipartUploadOutputStream;
    @Mock private ExternalCredentialsActor externalCredentialsActor;

    private Long requestId = 1L;
    private Long workflowId = 1L;
    private String clientAlias = "client_alias";
    private String scriptVariableName = "scriptVariableName";

    private Path path2;
    private List<URL> urls = new ArrayList<>();
    private CephIngestionHelper cephIngestionHelper;
    private List<String> files = new ArrayList<>();
    private List<ScriptVariable> scriptVariables = new ArrayList<>();
    private List<OutputLocation> outputLocationList = new ArrayList<>();
    private LinkedHashMap<String, SignalDataType> columnMapping = new LinkedHashMap<>();

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(URL.class);
        PowerMockito.mockStatic(Path.class);
        PowerMockito.mockStatic(ToolRunner.class);
        PowerMockito.mockStatic(AmazonS3Utils.class);
        MockitoAnnotations.initMocks(this);
        PowerMockito.mockStatic(MultipartUploadOutputStream.class);

        URL url = PowerMockito.mock(URL.class);
        String workflowName = "workflowName";
        String dataFrameName = "dataFrameName";
        String path1String = "hdfs://hadoopcluster2/ceph_base_hdfs_path/workflowName/scriptVariableName/ceph_path/refresh_id=1";
        String path2String = "hdfs://hadoopcluster2/ceph_base_hdfs_path/workflowName/scriptVariableName/ceph_path/refresh_id=1";
        String path3String = "/file_path/refresh_id=1/column=value/file1.csv";
        columnMapping.put("column",SignalDataType.TEXT);
        Path path1 = new Path(path1String);
        path2 = new Path(path2String);
        this.cephIngestionHelper = spy(new CephIngestionHelper(hdfsUtils, miscConfig, objectMapper, hadoopConfig, eventAuditUtil, externalCredentialsActor));

        urls.add(url);
        files.add(path3String);
        files.add("/file_path/refresh_id=1/column=value/file2.csv");
        scriptVariables.add(scriptVariable);
        outputLocationList.add(cephOutputLocation);

        when(workflow.getId()).thenReturn(workflowId);
        when(workflow.getName()).thenReturn(workflowName);
        when(workflowDetails.getWorkflow()).thenReturn(workflow);
        when(workflow.getWorkflowMeta()).thenReturn(workflowMeta);
        when(workflowDetails.getCephOutputs()).thenReturn(scriptVariables);
        when(workflowMeta.getHiveQueue()).thenReturn(PRODUCTION_HIVE_QUEUE);

        when(abstractDataFrame.getSeparator()).thenReturn(comma);
        when(scriptVariable.getName()).thenReturn(scriptVariableName);
        when(scriptVariable.getAdditionalVariable()).thenReturn(abstractDataFrame);
        when(scriptVariable.getOutputLocationDetailsList()).thenReturn(outputLocationList);

        when(cephOutputLocation.isMerged()).thenReturn(true);
        when(cephOutputLocation.getPath()).thenReturn("ceph_path");
        when(cephOutputLocation.getBucket()).thenReturn("ceph_bucket");
        when(cephOutputLocation.getClientAlias()).thenReturn(clientAlias);

        when(externalCredentials.getDetails()).thenReturn("partitionDetails");
        when(miscConfig.getSaltKey()).thenReturn("saltKey");
        when(miscConfig.getCephBaseHDFSPath()).thenReturn("/ceph_base_hdfs_path");
        when(hadoopConfig.getUser()).thenReturn("fk-ip-data-service");

        doNothing().when(inputStream).close();
        when(url.getPath()).thenReturn("/path");
        when(fileSystem.open(any())).thenReturn(inputStream);
        doNothing().when(multipartUploadOutputStream).close();
        when(path3.getFileSystem(any())).thenReturn(fileSystem);
        doNothing().when(cephIngestionHelper).copyStream(any(), any());
        when(hdfsUtils.getAllFilesUnderDirectory(path1)).thenReturn(files);
        doNothing().when(multipartUploadOutputStream).write(any(), anyInt(), anyInt());
        when(objectMapper.readValue("partitionDetails", CephEntity.class)).thenReturn(cephEntity);
        when(externalCredentialsActor.getCredentials(clientAlias)).thenReturn(externalCredentials);
        doNothing().when(eventAuditUtil).createCephIngestionEndInfoEvent(requestId, workflowId, dataFrameName, urls);
        doNothing().when(eventAuditUtil).createCephIngestionStartInfoEvent(requestId, workflowId, scriptVariableName, cephOutputLocation);

        PowerMockito.whenNew(Path.class).withArguments(path1String).thenReturn(path1);
        PowerMockito.whenNew(Path.class).withArguments(path2String).thenReturn(path2);
        PowerMockito.whenNew(Path.class).withArguments(path3String).thenReturn(path3);
        PowerMockito.whenNew(MultipartUploadOutputStream.class).withAnyArguments().thenReturn(multipartUploadOutputStream);
        PowerMockito.when(AmazonS3Utils.getCephUrls("saltKey", requestId, workflowName, dataFrameName, cephEntity, cephOutputLocation)).thenReturn(urls);
    }

    @Test
    public void testIngestInCephSuccessCase1() throws Exception {
        List<String> files = new ArrayList<>();
        files.add("/file_path/refresh_id=1/column=value/file1.csv");
        when(hdfsUtils.getAllFilesUnderDirectory(path2)).thenReturn(files);
        PowerMockito.when(ToolRunner.run(any(), any(), any())).thenReturn(0);

        cephIngestionHelper.ingestInCeph(requestId, workflowDetails);
        verify(workflowDetails, times(1)).getCephOutputs();
        verify(scriptVariable, times(1)).getOutputLocationDetailsList();
        verify(workflowDetails, times(1)).getWorkflow();
        verify(eventAuditUtil, times(1)).createCephIngestionStartInfoEvent(requestId, workflowId, scriptVariableName, cephOutputLocation);
        verify(workflow, times(2)).getId();
        verify(scriptVariable, times(4)).getName();
        verify(workflow, times(4)).getName();
        verify(cephOutputLocation, times(2)).getPath();
        verify(miscConfig, times(2)).getCephBaseHDFSPath();
        verify(cephOutputLocation, times(1)).getClientAlias();
        verify(externalCredentialsActor, times(1)).getCredentials(clientAlias);
        verify(externalCredentials, times(1)).getDetails();
        verify(objectMapper, times(1)).readValue("partitionDetails", CephEntity.class);
        verify(hadoopConfig, times(1)).getUser();
        verify(hdfsUtils, times(1)).getAllFilesUnderDirectory(any());
        verify(cephOutputLocation, times(1)).getBucket();
        verify(path3, times(1)).getFileSystem(any());
        verify(fileSystem, times(1)).open(any());
        verify(miscConfig, times(2)).getSaltKey();
        verify(cephOutputLocation, times(2)).isMerged();
        verify(inputStream, times(1)).close();
        verify(multipartUploadOutputStream, times(1)).close();
        verify(cephIngestionHelper, times(1)).copyStream(any(), any());
    }

    @Test
    public void testIngestInCephSuccessCase2() throws Exception {
        List<String> files = new ArrayList<>();
        files.add("/file_path/refresh_id=1/column=value/file1.csv");
        when(cephOutputLocation.getColumnMapping()).thenReturn(columnMapping);
        when(hdfsUtils.getAllFilesUnderDirectory(path2)).thenReturn(files);

        cephIngestionHelper.ingestInCeph(requestId, workflowDetails);
        verify(workflowDetails, times(1)).getCephOutputs();
        verify(scriptVariable, times(1)).getOutputLocationDetailsList();
        verify(workflowDetails, times(1)).getWorkflow();
        verify(eventAuditUtil, times(1)).createCephIngestionStartInfoEvent(requestId, workflowId, scriptVariableName, cephOutputLocation);
        verify(workflow, times(2)).getId();
        verify(scriptVariable, times(4)).getName();
        verify(workflow, times(4)).getName();
        verify(cephOutputLocation, times(2)).getPath();
        verify(miscConfig, times(2)).getCephBaseHDFSPath();
        verify(cephOutputLocation, times(1)).getClientAlias();
        verify(externalCredentialsActor, times(1)).getCredentials(clientAlias);
        verify(externalCredentials, times(1)).getDetails();
        verify(objectMapper, times(1)).readValue("partitionDetails", CephEntity.class);
        verify(hadoopConfig, times(1)).getUser();
        verify(hdfsUtils, times(1)).getAllFilesUnderDirectory(any());
        verify(cephOutputLocation, times(1)).getBucket();
        verify(path3, times(1)).getFileSystem(any());
        verify(fileSystem, times(1)).open(any());
        verify(miscConfig, times(2)).getSaltKey();
        verify(cephOutputLocation, times(2)).isMerged();
        verify(inputStream, times(1)).close();
        verify(multipartUploadOutputStream, times(1)).close();
        verify(cephIngestionHelper, times(1)).copyStream(any(), any());
        verify(hdfsUtils,times(1)).writeToFile(anyString(),anyString());
        verify(hdfsUtils,times(1)).rename(anyString(),anyString());
        verify(hdfsUtils,times(1)).concatFiles(any(),any());
    }

    @Test
    public void testIngestInCephCase1Failure() throws Exception {
        PowerMockito.when(ToolRunner.run(any(), any(), any())).thenReturn(1);
        cephIngestionHelper.ingestInCeph(requestId, workflowDetails);

        verify(workflowDetails, times(1)).getCephOutputs();
        verify(scriptVariable, times(1)).getOutputLocationDetailsList();
        verify(workflowDetails, times(1)).getWorkflow();
        verify(eventAuditUtil, times(1)).createCephIngestionStartInfoEvent(requestId, workflowId, scriptVariableName, cephOutputLocation);
        verify(workflow, times(2)).getId();
        verify(scriptVariable, times(5)).getName();
        verify(workflow, times(1)).getName();
        verify(cephOutputLocation, times(1)).getPath();
        verify(miscConfig, times(2)).getCephBaseHDFSPath();
        verify(cephOutputLocation, times(1)).getClientAlias();
        verify(externalCredentialsActor, times(1)).getCredentials(clientAlias);
        verify(externalCredentials, times(1)).getDetails();
        verify(objectMapper, times(1)).readValue("partitionDetails", CephEntity.class);
        verify(hadoopConfig, times(2)).getUser();
        verify(hdfsUtils, times(1)).getAllFilesUnderDirectory(any());
        verify(cephOutputLocation, times(1)).isMerged();
    }
    @Test
    public void testIngestInCephCase2Failure() throws Exception {
        PowerMockito.when(ToolRunner.run(any(), any(), any())).thenThrow(new Exception());
        List<String> actual = cephIngestionHelper.ingestInCeph(requestId, workflowDetails);
        assertNotNull(actual);
        assertEquals(actual.size(), 1);

        verify(workflowDetails, times(1)).getCephOutputs();
        verify(scriptVariable, times(1)).getOutputLocationDetailsList();
        verify(workflowDetails, times(1)).getWorkflow();
        verify(eventAuditUtil, times(1)).createCephIngestionStartInfoEvent(requestId, workflowId, scriptVariableName, cephOutputLocation);
        verify(workflow, times(2)).getId();
        verify(scriptVariable, times(5)).getName();
        verify(workflow, times(1)).getName();
        verify(cephOutputLocation, times(1)).getPath();
        verify(miscConfig, times(2)).getCephBaseHDFSPath();
        verify(cephOutputLocation, times(1)).getClientAlias();
        verify(externalCredentialsActor, times(1)).getCredentials(clientAlias);
        verify(externalCredentials, times(1)).getDetails();
        verify(objectMapper, times(1)).readValue("partitionDetails", CephEntity.class);
        verify(hadoopConfig, times(2)).getUser();
        verify(hdfsUtils, times(1)).getAllFilesUnderDirectory(any());
        verify(cephOutputLocation, times(1)).isMerged();
    }

    @Test
    public void testIngestInCephCase3Failure() throws Exception {
        PowerMockito.when(ToolRunner.run(any(), any(), any())).thenReturn(0);
        when(hdfsUtils.getAllFilesUnderDirectory(path2)).thenReturn(files);
        List<String> actual = cephIngestionHelper.ingestInCeph(requestId, workflowDetails);
        assertNotNull(actual);
        assertEquals(actual.size(), 1);

        verify(workflowDetails, times(1)).getCephOutputs();
        verify(scriptVariable, times(1)).getOutputLocationDetailsList();
        verify(workflowDetails, times(1)).getWorkflow();
        verify(eventAuditUtil, times(1)).createCephIngestionStartInfoEvent(requestId, workflowId, scriptVariableName, cephOutputLocation);
        verify(workflow, times(2)).getId();
        verify(scriptVariable, times(5)).getName();
        verify(workflow, times(1)).getName();
        verify(cephOutputLocation, times(1)).getPath();
        verify(miscConfig, times(2)).getCephBaseHDFSPath();
        verify(cephOutputLocation, times(1)).getClientAlias();
        verify(externalCredentialsActor, times(1)).getCredentials(clientAlias);
        verify(externalCredentials, times(1)).getDetails();
        verify(objectMapper, times(1)).readValue("partitionDetails", CephEntity.class);
        verify(hadoopConfig, times(2)).getUser();
        verify(hdfsUtils, times(2)).getAllFilesUnderDirectory(any());
        verify(cephOutputLocation, times(1)).isMerged();
    }

    @Test
    public void testIngestInCephCase4Failure() throws Exception {
        when(cephOutputLocation.isMerged()).thenReturn(false);
        doThrow(new IOException()).when(cephIngestionHelper).copyStream(any(), any());
        doNothing().when(eventAuditUtil).createCephIngestionErrorEvent(anyLong(), anyLong(), anyString(), anyString(), any());

        cephIngestionHelper.ingestInCeph(requestId, workflowDetails);

        verify(workflowDetails, times(1)).getCephOutputs();
        verify(scriptVariable, times(1)).getOutputLocationDetailsList();
        verify(workflowDetails, times(1)).getWorkflow();
        verify(eventAuditUtil, times(1)).createCephIngestionStartInfoEvent(requestId, workflowId, scriptVariableName, cephOutputLocation);
        verify(workflow, times(2)).getId();
        verify(scriptVariable, times(5)).getName();
        verify(workflow, times(2)).getName();
        verify(cephOutputLocation, times(2)).getPath();
        verify(cephOutputLocation, times(1)).getBucket();
        verify(miscConfig, times(1)).getCephBaseHDFSPath();
        verify(cephOutputLocation, times(1)).getClientAlias();
        verify(externalCredentialsActor, times(1)).getCredentials(clientAlias);
        verify(externalCredentials, times(1)).getDetails();
        verify(objectMapper, times(1)).readValue("partitionDetails", CephEntity.class);
        verify(hadoopConfig, times(1)).getUser();
        verify(hdfsUtils, times(1)).getAllFilesUnderDirectory(any());
        verify(miscConfig, times(1)).getSaltKey();
        verify(cephOutputLocation, times(2)).isMerged();
        verify(path3, times(1)).getFileSystem(any());
        verify(fileSystem, times(1)).open(any());
        verify(cephIngestionHelper, times(1)).copyStream(any(), any());
        verify(eventAuditUtil, times(1)).createCephIngestionErrorEvent(anyLong(), anyLong(), anyString(), anyString(), any());
    }

    @Test
    public void testIngestInCephCase5Failure() throws Exception {
        PowerMockito.when(ToolRunner.run(any(), any(), any())).thenReturn(1);
        when(cephOutputLocation.getColumnMapping()).thenReturn(columnMapping);
        cephIngestionHelper.ingestInCeph(requestId, workflowDetails);

        verify(workflowDetails, times(1)).getCephOutputs();
        verify(scriptVariable, times(1)).getOutputLocationDetailsList();
        verify(workflowDetails, times(1)).getWorkflow();
        verify(eventAuditUtil, times(1)).createCephIngestionStartInfoEvent(requestId, workflowId, scriptVariableName, cephOutputLocation);
        verify(workflow, times(2)).getId();
        verify(scriptVariable, times(5)).getName();
        verify(workflow, times(1)).getName();
        verify(cephOutputLocation, times(1)).getPath();
        verify(miscConfig, times(2)).getCephBaseHDFSPath();
        verify(cephOutputLocation, times(1)).getClientAlias();
        verify(externalCredentialsActor, times(1)).getCredentials(clientAlias);
        verify(externalCredentials, times(1)).getDetails();
        verify(objectMapper, times(1)).readValue("partitionDetails", CephEntity.class);
        verify(hadoopConfig, times(2)).getUser();
        verify(hdfsUtils, times(1)).getAllFilesUnderDirectory(any());
        verify(cephOutputLocation, times(1)).isMerged();
    }
}
