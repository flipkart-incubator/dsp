package com.flipkart.dsp.executor.persist;

import com.flipkart.dsp.client.MultiDatastoreClient;
import com.flipkart.dsp.config.HadoopConfig;
import com.flipkart.dsp.dto.MultiDataStorePutResponse;
import com.flipkart.dsp.exceptions.MultiDataStoreClientException;
import com.flipkart.dsp.executor.exception.PersistenceException;
import com.flipkart.dsp.executor.utils.LocationManager;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.ScriptVariable;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class VariablePersisterTest {
    private VariablePersister variablePersister;
    private Set<ScriptVariable> scriptVariables = new HashSet<>();

    @Mock private ScriptVariable scriptVariable;
    @Mock private ModelPersister modelPersister;
    @Mock private LocationManager locationManager;
    @Mock private DataFramePersister dataFramePersister;
    @Mock private MultiDatastoreClient multiDatastoreClient;
    @Mock private HadoopConfig hadoopConfig;
    @Mock private MultiDataStorePutResponse multiDataStorePutResponse;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        variablePersister = new VariablePersister(hadoopConfig, modelPersister, locationManager, dataFramePersister, multiDatastoreClient);

        scriptVariables.add(scriptVariable);
        when(hadoopConfig.getUser()).thenReturn("fk-ip-data-service");
        when(scriptVariable.getDataType()).thenReturn(DataType.BYTEARRAY);
        when(locationManager.getHDFSIntermediateFolderPath()).thenReturn("/folder_path");
    }

    @Test
    public void testPersist() throws PersistenceException, TException, TableNotFoundException {
        variablePersister.persist("","",new HashSet<>(), new ArrayList<>(),1l,"");
        verify(dataFramePersister).persist(any(),any(),any(),any(),any(), anyString());
        verify(modelPersister).persist(any(),any(),any());
    }

    @Test
    public void testMoveIntermediateVariablesToHDFSSuccess() throws Exception {
        when(multiDatastoreClient.put(any())).thenReturn(multiDataStorePutResponse);
        variablePersister.moveIntermediateVariablesToHDFS(scriptVariables);

        verify(scriptVariable, times(1)).getDataType();
        verify(locationManager, times(1)).getHDFSIntermediateFolderPath();
        verify(multiDatastoreClient, times(1)).put(any());
    }

    @Test
    public void testMoveIntermediateVariablesToHDFSFailure() throws Exception {
        boolean isException = false;
        when(multiDatastoreClient.put(any())).thenThrow(new MultiDataStoreClientException("Error"));

        try {
            variablePersister.moveIntermediateVariablesToHDFS(scriptVariables);
        } catch (PersistenceException e) {
            isException = true;
            assertTrue(e.getMessage().contains("Failed to move data from "));
        }

        assertTrue(isException);
        verify(scriptVariable, times(1)).getDataType();
        verify(locationManager, times(1)).getHDFSIntermediateFolderPath();
        verify(multiDatastoreClient, times(1)).put(any());
        verify(hadoopConfig, times(1)).getUser();
    }

}
