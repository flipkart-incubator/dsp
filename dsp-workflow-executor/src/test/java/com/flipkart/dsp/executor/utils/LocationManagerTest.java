package com.flipkart.dsp.executor.utils;

import com.flipkart.dsp.config.HadoopConfig;
import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.outputVariable.*;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class LocationManagerTest {
    @Mock private MiscConfig miscConfig;
    @Mock private HadoopConfig hadoopConfig;
    @Mock private ScriptVariable scriptVariable;
    @Mock private LocationHelper locationHelper;
    @Mock private MetaStoreClient metaStoreClient;
    @Mock private AbstractDataFrame additionalVariable;

    private LocationManager locationManager;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        this.locationManager = spy(new LocationManager(miscConfig, hadoopConfig, locationHelper, metaStoreClient));

        when(hadoopConfig.getBasePath()).thenReturn("/base_path");
        when(locationHelper.getLocalPath()).thenReturn("/local_path");
        when(scriptVariable.getName()).thenReturn("script_variable_name");
        when(scriptVariable.getDataType()).thenReturn(DataType.DATAFRAME);
        when(scriptVariable.getAdditionalVariable()).thenReturn(additionalVariable);
        when(miscConfig.getCephBaseHDFSPath()).thenReturn("/ceph_base_path");
        when(hadoopConfig.getModelRepoLocation()).thenReturn("/model_repo_location");
    }

    @Test
    public void testGetLocalFilePath() {
        locationManager.getLocalFilePath(scriptVariable);
        verify(scriptVariable, times(1)).getDataType();
        verify(scriptVariable, times(1)).getName();
    }

    @Test
    public void testBuildHDFSPartitionLocation() {
        String baseLocation = "/base_location";
        LinkedHashMap<String, String> partitionGranularity = new LinkedHashMap<>();
        partitionGranularity.put("column1","value1");
        String actual = locationManager.buildHDFSPartitionLocation(baseLocation, partitionGranularity);
        assertEquals(actual, baseLocation + "/column1=value1");
    }

    @Test
    public void testPutPartitionsInPath() {
        List<String> partitions = new ArrayList<>();
        partitions.add("column1");

        String actual = locationManager.putPartitionsInPath(partitions, 1L, "run_time_name", "variable_name");
        assertTrue(actual.contains( "run_time_name/dataframes/column1/variable_name"));
        verify(locationHelper, times(1)).getLocalPath();
    }

    @Test
    public void testGetHDFSIntermediateFolderPath() {
        assertTrue(locationManager.getHDFSIntermediateFolderPath().contains("/base_path"));
        verify(hadoopConfig, times(1)).getBasePath();
    }

    @Test
    public void testGetDefaultHDFSOutputLocation() {
        assertEquals(locationManager.getDefaultHDFSOutputLocation("id"),"/base_path"+ "/id");
        verify(hadoopConfig, times(1)).getBasePath();
    }

    @Test
    public void testGetModelStorageLocation() {
        String actual = locationManager.getModelStorageLocation("workflow_name","id");
        assertEquals(actual,"/model_repo_location"+ "/workflow_name/id");
        verify(hadoopConfig, times(1)).getModelRepoLocation();
    }

    @Test
    public void testGetWorkflowOutputLocationCase1() throws Exception {
        when(additionalVariable.getHiveTable()).thenReturn("hive_table");
        when(metaStoreClient.getTableLocation(any())).thenReturn("hdfs_location");

        List<String> actual = locationManager.getWorkflowOutputLocation(scriptVariable, "pipeline_id", "workflow_name");
        assertNotNull(actual);
        assertEquals(actual.size(), 1);
        verify(scriptVariable, times(1)).getAdditionalVariable();
        verify(additionalVariable, times(2)).getHiveTable();
        verify(metaStoreClient, times(1)).getTableLocation(additionalVariable.getHiveTable());
    }

    @Test
    public void testGetWorkflowOutputLocationCase2() throws Exception {
        List<OutputLocation> outputLocations = getOutputLocations();
        when(scriptVariable.getOutputLocationDetailsList()).thenReturn(outputLocations);
        when(additionalVariable.getHiveTable()).thenReturn(null);
        when(metaStoreClient.getTableLocation("hadoopcluster_db.fkint_bigfoot_test_fdp_entity_name_20"))
                .thenReturn("/test_path/hadoopcluster_db.fkint_bigfoot_test_fdp_entity_name_20");
        when(metaStoreClient.getTableLocation("test_db.test_table")).thenReturn("/test_path/test_db.test_table");

        List<String> actual = locationManager.getWorkflowOutputLocation(scriptVariable, "pipeline_id", "workflow_name");
        assertNotNull(actual);
        assertEquals(actual.size(), 6);
        assertEquals(actual.get(0), "/test_path/hadoopcluster_db.fkint_bigfoot_test_fdp_entity_name_20");
        assertEquals(actual.get(1),"/test_path/test_db.test_table");
        assertEquals(actual.get(2),"/test_path/test_db.test_table");
        assertEquals(actual.get(3),"/base_path/pipeline_id");
        assertEquals(actual.get(4),"/location");
        assertEquals(actual.get(5),"/ceph_base_path/workflow_name/script_variable_name//path");

        verify(scriptVariable, times(1)).getAdditionalVariable();
        verify(scriptVariable, times(1)).getOutputLocationDetailsList();
        verify(hadoopConfig, times(1)).getBasePath();
        verify(miscConfig, times(1)).getCephBaseHDFSPath();
        verify(metaStoreClient, times(3)).getTableLocation(any());
    }

    private List<OutputLocation> getOutputLocations() {
        List<OutputLocation> outputLocations = new ArrayList<>();
        HiveOutputLocation hiveOutputLocation = new HiveOutputLocation();
        hiveOutputLocation.setDatabase("test_db");
        hiveOutputLocation.setTable("test_table");

        HDFSOutputLocation hdfsOutputLocation = new HDFSOutputLocation();
        HDFSOutputLocation hdfsOutputLocation1 = new HDFSOutputLocation();
        hdfsOutputLocation.setLocation(null);
        hdfsOutputLocation1.setLocation("/location");

        CephOutputLocation cephOutputLocation = new CephOutputLocation();
        cephOutputLocation.setPath("/path");

        outputLocations.add(hiveOutputLocation);
        outputLocations.add(hdfsOutputLocation);
        outputLocations.add(hdfsOutputLocation1);
        outputLocations.add(cephOutputLocation);
        return outputLocations;
    }

}
