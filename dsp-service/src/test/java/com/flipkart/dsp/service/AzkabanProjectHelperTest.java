//package com.flipkart.dsp.service;
//
//import com.flipkart.dsp.api.AzkabanExecutionAPI;
//import com.flipkart.dsp.api.ConfigServiceAPI;
//import com.flipkart.dsp.config.DSPServiceConfig.ServiceConfig;
//import com.flipkart.dsp.config.IPPDSPConfiguration.AzkabanConfig;
//import com.flipkart.dsp.db.entities.WorkflowEntity;
//import com.flipkart.dsp.exceptions.AzkabanException;
//import com.flipkart.dsp.exception.AzkabanProjectCreationException;
//import com.flipkart.kloud.config.error.ConfigServiceException;
//import org.apache.commons.io.FileUtils;
//import org.javatuples.Pair;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//import org.zeroturnaround.zip.ZipUtil;
//
//import java.io.File;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.UUID;
//
//import static com.flipkart.dsp.utils.Constants.underscore;
//import static org.junit.Assert.*;
//import static org.mockito.Mockito.*;
//
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({AzkabanProjectHelper.class, Paths.class, File.class, ZipUtil.class, FileUtils.class, Files.class})
//public class AzkabanProjectHelperTest {
//
//    @Mock private Path azkZipFile;
//    @Mock private WorkflowEntity workflowEntity;
//    @Mock private WorkflowGroup workflowGroup;
//    @Mock private ServiceConfig serviceConfig;
//    @Mock private AzkabanConfig azkabanConfig;
//    @Mock private ConfigServiceAPI configServiceAPI;
//    @Mock private AzkabanExecutionAPI azkabanExecutionAPI;
//
//    private String env = "stage-beta";
//    private String basePath = "/tmp/";
//    private String projectName = "testProject";
//    private String description = "Test Project";
//    private String workflowName = "testWorkflow";
//    private AzkabanProjectHelper azkabanProjectHelper;
//    private List<Pair<WorkflowEntity, WorkflowEntity>> workflowPairList = new ArrayList<>();
//
//    @Before
//    public void setup() {
//        MockitoAnnotations.initMocks(this);
//        azkabanProjectHelper = spy(new AzkabanProjectHelper(serviceConfig, configServiceAPI, azkabanConfig, azkabanExecutionAPI));
//
//        PowerMockito.mockStatic(Paths.class);
//        PowerMockito.mockStatic(ZipUtil.class);
//        workflowPairList.add(new Pair<>(workflowEntity, workflowEntity));
//    }
//
//
//    @Test
//    public void testCreateAndUploadAzkabanProjectSuccess() throws Exception {
//        doNothing().when(azkabanExecutionAPI).createProject(projectName, description);
//        doNothing().when(azkabanExecutionAPI).uploadProject(anyString(), anyString());
//
//        azkabanProjectHelper.createAndUploadAzkabanProject(projectName, description, azkZipFile);
//        verify(azkabanExecutionAPI, times(1)).createProject(projectName, description);
//        verify(azkabanExecutionAPI, times(1)).uploadProject(anyString(), anyString());
//    }
//
//    @Test
//    public void testCreateAndUploadAzkabanProjectFailureCase1() throws Exception {
//        boolean isException = false;
//        doThrow(new AzkabanException("Exception")).when(azkabanExecutionAPI).createProject(projectName, description);
//
//        try {
//            azkabanProjectHelper.createAndUploadAzkabanProject(projectName, description, azkZipFile);
//        } catch (AzkabanProjectCreationException e) {
//            isException = true;
//            assertEquals(e.getMessage(), "Could not create project in Azkaban : " + projectName);
//        }
//
//        assertTrue(isException);
//        verify(azkabanExecutionAPI, times(1)).createProject(projectName, description);
//    }
//
//    @Test
//    public void testCreateAndUploadAzkabanProjectFailureCase2() throws Exception {
//        boolean isException = false;
//        doNothing().when(azkabanExecutionAPI).createProject(projectName, description);
//        doThrow(new AzkabanException("Exception")).when(azkabanExecutionAPI).uploadProject(anyString(), anyString());
//        try {
//            azkabanProjectHelper.createAndUploadAzkabanProject(projectName, description, azkZipFile);
//        } catch (AzkabanProjectCreationException e) {
//            isException = true;
//            assertEquals(e.getMessage(), "Could not upload zip file into Azkaban Project : " + projectName);
//        }
//
//        assertTrue(isException);
//        verify(azkabanExecutionAPI, times(1)).createProject(projectName, description);
//        verify(azkabanExecutionAPI, times(1)).uploadProject(anyString(), anyString());
//    }
//
//    @Test
//    public void testCreateZipFile() {
//        when(serviceConfig.getTempDir()).thenReturn("/tmp");
//        PowerMockito.when(Paths.get(anyString(), anyString())).thenReturn(azkZipFile);
//
//        Path expected = azkabanProjectHelper.createZipFile("/tmp", UUID.randomUUID().toString());
//        assertNotNull(expected);
//        verify(serviceConfig, times(1)).getTempDir();
//    }
//
//    @Test
//    public void testCreateJobsForWorkflowGroupSuccess() throws Exception {
//        String azkJarVersion = "100.0";
//        when(workflowEntity.getId()).thenReturn(11L);
//        when(workflowEntity.getName()).thenReturn(workflowName);
//        when(azkabanConfig.getEnvironment()).thenReturn(env);
//        when(azkabanConfig.getAzkJarVersion()).thenReturn(azkJarVersion);
//
//        azkabanProjectHelper.createJobsForWorkflowGroup(workflowPairList, basePath);
//        verifyJobFile(env, azkJarVersion, "OTS", "AzkabanOTSNode", basePath, workflowName);
//        verifyJobFile(env, azkJarVersion, "SG", "AzkabanSGNode", basePath, workflowName);
//        verifyJobFile(env, azkJarVersion, "WF", "AzkabanWorkflowNode", basePath, workflowName);
//        verifyJobFile(env, azkJarVersion, "OI", "AzkabanOutputIngestionNode", basePath, workflowName);
//        verifyJobFile(env, azkJarVersion, "TERMINAL", "AzkabanTerminalNode", basePath, workflowName);
//
//        verify(workflowEntity, times(8)).getId();
//        verify(workflowEntity, times(8)).getName();
//        verify(azkabanConfig, times(8)).getEnvironment();
//    }
//
//    @Test
//    public void testCreateJobsForWorkflowGroupFailureCase1() {
//        boolean isException = false;
//
//        try {
//            azkabanProjectHelper.createJobsForWorkflowGroup(new ArrayList<>(), basePath);
//        } catch (AzkabanProjectCreationException | IllegalArgumentException e) {
//            isException = true;
//            assertEquals(e.getMessage(), "Cannot construct jobs out of empty map");
//        }
//        assertTrue(isException);
//    }
//
//    @Test
//    @Ignore
//    public void testCreateJobsForWorkflowGroupFailureCase2() throws Exception {
//        when(workflowEntity.getId()).thenReturn(11L);
//        when(workflowEntity.getName()).thenReturn(workflowName);
//        when(azkabanConfig.getEnvironment()).thenReturn(env);
//        when(azkabanConfig.getAzkJarVersion()).thenThrow(new ConfigServiceException("Exception", ConfigServiceException.TYPE.NOT_FOUND));
//
//        boolean isException = false;
//
//        try {
//            azkabanProjectHelper.createJobsForWorkflowGroup(workflowPairList, basePath);
//        } catch (AzkabanProjectCreationException | IllegalArgumentException e) {
//            isException = true;
//            assertEquals(e.getMessage(), "Failed to create azkaban job for workflowEntity group");
//        }
//        assertTrue(isException);
//        verify(azkabanConfig, times(1)).getEnvironment();
//        verify(configServiceAPI, times(1)).getAzkabanJarVersion(env);
//    }
//
//    @Test
//    public void testCleanupGeneratedJobsSuccess() {
//        PowerMockito.mockStatic(FileUtils.class);
//        PowerMockito.mockStatic(Files.class);
//        azkabanProjectHelper.cleanupGeneratedJobs(basePath, azkZipFile);
//    }
//
//    @Test
//    public void testGetAzkabanProjectName() {
//        double version = 1.0;
//        String workflowGroupName = "testWorkflowGroup";
//
//        when(azkabanConfig.getEnvironment()).thenReturn(env);
//        when(workflowGroup.getName()).thenReturn(workflowGroupName);
//        String expected = azkabanProjectHelper.getAzkabanProjectName(workflowGroup, version, false);
//        System.out.println(expected);
//        assertEquals(expected, "TESTWORKFLOWGROUP_1_STAGE-BETA");
//
//        expected = azkabanProjectHelper.getAzkabanProjectName(workflowGroup, version, true);
//        System.out.println(expected);
//        assertEquals(expected, "DRAFT_TESTWORKFLOWGROUP_1_STAGE-BETA");
//    }
//
//    private void verifyJobFile(String env, String azkJarVersion,String jobType, String jobClass, String basePath, String workflowName) throws Exception {
//        String jobFile = FileUtils.readFileToString(new File(basePath + workflowName + underscore + jobType + ".job"));
//        String expectedClassPath = "/grid/dsp-azkaban-jobs/" + env + "/" + azkJarVersion + "/dsp-azkaban-jobs-1.0-SNAPSHOT-" + azkJarVersion + ".jar";
//        String expectedApplicationClass = "com.flipkart.dsp.jobs." + jobClass;
//
//        String actualClassPath = jobFile.substring(jobFile.indexOf("classpath") + "classpath".length() + 1, jobFile.indexOf("\n", jobFile.indexOf("classpath")));
//        String actualApplicationClass = jobFile.substring(jobFile.indexOf("JobDriver.applicationClass") + "JobDriver.applicationClass".length() + 1,
//                jobFile.indexOf("\n", jobFile.indexOf("JobDriver.applicationClass")));
//
//        assertTrue(actualClassPath.contains(expectedClassPath));
//        assertEquals(expectedApplicationClass, actualApplicationClass);
//    }
//
//}