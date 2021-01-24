package com.flipkart.dsp.executor.persist;

import com.flipkart.dsp.client.MultiDatastoreClient;
import com.flipkart.dsp.entities.misc.ConfigPayload;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.executor.exception.DataframePersistenceException;
import com.flipkart.dsp.executor.utils.LocationManager;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.models.outputVariable.OutputLocation;
import com.flipkart.dsp.models.variables.AbstractDataFrame;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 */
public class DataFramePersisterTest {

    @Mock Workflow workflow;
    @Mock ConfigPayload configPayload;
    @Mock WorkflowDetails workflowDetails;
    @Mock LocationManager locationManager;
    @Mock AbstractDataFrame additionalVariables;
    @Mock MultiDatastoreClient multiDatastoreClient;

    private String hiveTable = "test_table";
    private String workflowName = "workflowName";
    private DataFramePersister dataFramePersister;
    private List<String> outputLocationList = new ArrayList<>();
    private Set<ScriptVariable> scriptVariables = new HashSet<>();
    private List<String> partitionMapping = new ArrayList<>();

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        this.dataFramePersister = spy(new DataFramePersister(locationManager, multiDatastoreClient));

        Long refresh_id = 12L;
        final ArrayList<OutputLocation> outputLocations = new ArrayList<>();
        ScriptVariable scriptVariable = new ScriptVariable("output_var", DataType.DATAFRAME, "value", additionalVariables, outputLocations, false);
        scriptVariables.add(scriptVariable);
        partitionMapping.add("input_partition");
        outputLocationList.add("output_location");

        when(configPayload.getScope()).thenReturn("[{\"id\" : 1, \"type\" : \"IN\", \"values_in\" : [1]}]");
        when(workflowDetails.getWorkflow()).thenReturn(workflow);
        when(configPayload.getRefreshId()).thenReturn(refresh_id);
        when(locationManager.getWorkflowOutputLocation(any(), any(), any())).thenReturn(outputLocationList);
    }

    // Success DDP Output
    @Test
    public void testPersistSuccessCase1() throws Exception {
        when(additionalVariables.getHiveTable()).thenReturn(hiveTable);
        when(locationManager.buildHDFSPartitionLocation(anyString(), any())).thenReturn("partition_location");

        dataFramePersister.persist(configPayload.getPipelineExecutionId(),scriptVariables,partitionMapping,configPayload.getRefreshId(), configPayload.getScope(), workflowName);
        verify(configPayload, times(1)).getScope();
        verify(locationManager, times(1)).getWorkflowOutputLocation(any(), any(), any());
        verify(locationManager, times(1)).buildHDFSPartitionLocation(anyString(), any());
    }

    @Test
    public void testPersistSuccessCase2() throws Exception {
        String hadoopclusterDb = "dsp_output";
        when(additionalVariables.getHiveTable()).thenReturn(null);
        when(locationManager.buildHDFSPartitionLocation(anyString(), any())).thenReturn("partition_location");

        dataFramePersister.persist(configPayload.getPipelineExecutionId(),scriptVariables,partitionMapping,configPayload.getRefreshId(), configPayload.getScope(), workflowName);
        verify(configPayload, times(1)).getScope();
        verify(locationManager, times(1)).getWorkflowOutputLocation(any(), any(), any());
        verify(locationManager, times(1)).buildHDFSPartitionLocation(anyString(), any());
    }

    @Test
    public void testPersistFailure() throws Exception {
        when(additionalVariables.getHiveTable()).thenReturn(hiveTable);
        when(locationManager.getWorkflowOutputLocation(any(), any(), anyString())).thenThrow(new HiveClientException("Error"));

        boolean isException = false;
        try {
            dataFramePersister.persist(configPayload.getPipelineExecutionId(),scriptVariables,partitionMapping,configPayload.getRefreshId(), configPayload.getScope(), workflowName);
        } catch (DataframePersistenceException e) {
            isException = true;
            assertTrue(e.getMessage().contains("Failed to persist scriptVariable: "));
        }

        assertTrue(isException);
    }
}
