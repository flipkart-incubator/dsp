package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.overrides.CSVDataframeOverride;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.sg.override.OverrideManager;
import com.flipkart.dsp.utils.Constants;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static com.flipkart.dsp.utils.Constants.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class SGTypeHelperTest {
    @Mock private Signal signal1;
    @Mock private Signal signal2;
    @Mock private DataFrame dataFrame;
    @Mock private DataTable dataTable;
    @Mock private DataSource dataSource;
    @Mock private SignalGroup signalGroup;
    @Mock private PartitionHelper partitionHelper;
    @Mock private OverrideManager overrideManager;
    @Mock private MetaStoreClient metaStoreClient;
    @Mock private DataFrameConfig dataFrameConfig;
    @Mock private SignalGroup.SignalMeta signalMeta1;
    @Mock private SignalGroup.SignalMeta signalMeta2;
    @Mock private SignalDefinition signalDefinition;
    @Mock private DataFrameGenerateRequest dataFrameGenerateRequest;
    @Mock private CSVDataframeOverride csvDataframeOverride;

    private SGTypeHelper sgTypeHelper;
    private String dataSourceId = "test_db";
    private String dataTableId = "test_table";
    private String baseEntity1 = "baseEntity1";
    private String baseEntity2 = "baseEntity2";
    private String dataFrameName = "testDataFrameName";
    private ArrayList<String> columns = new ArrayList<>();
    private ArrayList<String> partitions = new ArrayList<>();
    private Set<String> existingPartitions = new HashSet<>();

    private LinkedHashSet<Signal> signals = new LinkedHashSet<>();
    private String fullTableName = dataSourceId + dot + dataTableId;
    private Set<DataFrameScope> dataFrameScopes = new HashSet<>();
    private List<SignalGroup.SignalMeta> signalMetas = new ArrayList<>();
    private Map<String, DataType> inputDataFrameType = new HashMap<>();
    private Map<String, DataframeOverride> dataframeOverrideMap = new HashMap<>();


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        this.sgTypeHelper = spy(new SGTypeHelper(partitionHelper, overrideManager, metaStoreClient));

        signals.add(signal1);
        columns.add(baseEntity1);
        columns.add(Constants.REFRESH_ID);
        partitions.add(baseEntity1);
        signalMetas.add(signalMeta1);
        inputDataFrameType.put(dataFrameName, DataType.DATAFRAME);
        dataframeOverrideMap.put(dataFrameName, csvDataframeOverride);

        when(dataTable.getId()).thenReturn(dataTableId);
        when(dataTable.getDataSource()).thenReturn(dataSource);

        when(signalMeta1.getSignal()).thenReturn(signal1);
        when(signalMeta2.getSignal()).thenReturn(signal2);
        when(signalMeta1.getDataTable()).thenReturn(dataTable);
        when(signalMeta2.getDataTable()).thenReturn(dataTable);
        when(signalGroup.getSignalMetas()).thenReturn(signalMetas);

        when(signal1.getSignalBaseEntity()).thenReturn(baseEntity1);
        when(signal2.getSignalBaseEntity()).thenReturn(baseEntity2);
        when(signal1.getSignalDefinition()).thenReturn(signalDefinition);
        when(signal2.getSignalDefinition()).thenReturn(signalDefinition);
        when(signalDefinition.getSignalValueType()).thenReturn(SignalValueType.ONE_TO_ONE);

        when(dataSource.getId()).thenReturn(dataSourceId);
        when(dataFrame.getName()).thenReturn(dataFrameName);
        when(dataFrame.getPartitions()).thenReturn(partitions);
        when(dataFrame.getSignalGroup()).thenReturn(signalGroup);
        when(dataFrameConfig.getVisibleSignals()).thenReturn(signals);
        when(dataFrame.getDataFrameConfig()).thenReturn(dataFrameConfig);
        when(dataFrameGenerateRequest.getInputDataFrameType()).thenReturn(inputDataFrameType);
        when(dataFrameGenerateRequest.getDataFrameOverrideMap()).thenReturn(dataframeOverrideMap);

        when(partitionHelper.doesPartitionMatches(any(), any())).thenReturn(true);
        when(metaStoreClient.getColumnNames(fullTableName)).thenReturn(columns);
        when(metaStoreClient.getPartitionedColumnNames(fullTableName)).thenReturn(existingPartitions);
        when(metaStoreClient.getHiveTableStorageFormat(fullTableName)).thenReturn("org.apache.hadoop.mapred.TextInputFormat");
        when(overrideManager.getDataTableForOverride(dataFrame, csvDataframeOverride, dataTable)).thenReturn(dataTable);
    }

    //FULL_QUERY
    @Test
    public void testCalculateSGTypeCase1() throws Exception {
        when(signalDefinition.getSignalValueType()).thenReturn(SignalValueType.CONDITIONAL);
        SGType sgType = sgTypeHelper.calculateSGType(dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        assertEquals(sgType, SGType.FULL_QUERY);
        verify(dataFrame).getName();
        verify(dataFrame, times(2)).getSignalGroup();
        verify(signalGroup, times(2)).getSignalMetas();
        verify(signalMeta1).getDataTable();
        verify(signalMeta1).getSignal();
        verify(signal1).getSignalDefinition();
        verify(signalDefinition).getSignalValueType();
    }

    //SINGLE_TABLE_QUERY
    @Test
    public void testCalculateSGTypeCase2() throws Exception {
        fullTableName = HADOOP_QUERY_DATABASE + dot + dataTableId;
        when(dataSource.getId()).thenReturn(HADOOP_QUERY_DATABASE);
        when(metaStoreClient.getHiveTableStorageFormat(fullTableName)).thenReturn("org.apache.hadoop.mapred.TextInputFormat");
        SGType sgType = sgTypeHelper.calculateSGType(dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        assertEquals(sgType, SGType.SINGLE_TABLE_QUERY);

        verify(dataFrame).getName();
        verify(dataFrame, times(5)).getSignalGroup();
        verify(signalGroup, times(5)).getSignalMetas();
        verify(signalMeta1, times(2)).getDataTable();
        verify(signalMeta1, times(2)).getSignal();
        verify(signal1).getSignalDefinition();
        verify(signalDefinition).getSignalValueType();
        verify(signal1).getSignalBaseEntity();
        verify(dataFrameGenerateRequest, times(3)).getDataFrameOverrideMap();
        verify(overrideManager).getDataTableForOverride(dataFrame, csvDataframeOverride, dataTable);
        verify(dataTable).getDataSource();
        verify(dataSource).getId();
    }

    //SINGLE_TABLE_QUERY
    @Test
    public void testCalculateSGTypeCase3() throws Exception {
        columns.clear();
        partitions.clear();
        signals.add(signal2);
        columns.add(baseEntity2);
        columns.add(baseEntity1);
        columns.add(Constants.REFRESH_ID);
        signalMetas.add(signalMeta2);

        when(dataFrameConfig.getVisibleSignals()).thenReturn(signals);
        when(metaStoreClient.getColumnNames(fullTableName)).thenReturn(columns);
        SGType sgType = sgTypeHelper.calculateSGType(dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        assertEquals(sgType, SGType.SINGLE_TABLE_QUERY);

        verify(dataFrame).getName();
        verify(dataFrame, times(5)).getSignalGroup();
        verify(signalGroup, times(5)).getSignalMetas();
        verify(signalMeta1, times(2)).getDataTable();
        verify(signalMeta1, times(2)).getSignal();
        verify(signalMeta2, times(1)).getDataTable();
        verify(signalMeta2, times(2)).getSignal();
        verify(signal1).getSignalDefinition();
        verify(signal2).getSignalDefinition();
        verify(signalDefinition, times(2)).getSignalValueType();
        verify(signal1, times(4)).getSignalBaseEntity();
        verify(signal2, times(4)).getSignalBaseEntity();
        verify(dataFrameGenerateRequest, times(3)).getDataFrameOverrideMap();
        verify(overrideManager).getDataTableForOverride(dataFrame, csvDataframeOverride, dataTable);
        verify(dataTable).getDataSource();
        verify(dataSource).getId();
        verify(metaStoreClient).getColumnNames(fullTableName);
        verify(dataFrame).getPartitions();
        verify(metaStoreClient).getPartitionedColumnNames(fullTableName);
        verify(metaStoreClient).getHiveTableStorageFormat(fullTableName);
        verify(partitionHelper).doesPartitionMatches(any(), any());
        verify(dataFrame).getDataFrameConfig();
        verify(dataFrameConfig).getVisibleSignals();
    }

    // NO_QUERY
    @Test
    public void testCalculateSGTypeCase4() throws Exception {
        partitions.clear();
        partitions.add(dataFrameName);
        when(dataFrame.getPartitions()).thenReturn(partitions);

        SGType sgType = sgTypeHelper.calculateSGType(dataFrame, dataFrameGenerateRequest, dataFrameScopes);
        assertEquals(sgType, SGType.NO_QUERY);

        verify(dataFrame).getName();
        verify(dataFrame, times(5)).getSignalGroup();
        verify(signalGroup, times(5)).getSignalMetas();
        verify(signalMeta1, times(2)).getDataTable();
        verify(signalMeta1, times(2)).getSignal();
        verify(signal1).getSignalDefinition();
        verify(signalDefinition).getSignalValueType();
        verify(signal1, times(3)).getSignalBaseEntity();
        verify(dataFrameGenerateRequest, times(3)).getDataFrameOverrideMap();
        verify(overrideManager).getDataTableForOverride(dataFrame, csvDataframeOverride, dataTable);
        verify(dataTable).getDataSource();
        verify(dataSource).getId();
        verify(metaStoreClient).getColumnNames(fullTableName);
        verify(dataFrame).getPartitions();
        verify(metaStoreClient).getPartitionedColumnNames(fullTableName);
        verify(metaStoreClient).getHiveTableStorageFormat(fullTableName);
        verify(partitionHelper).doesPartitionMatches(any(), any());
        verify(dataFrame).getDataFrameConfig();
        verify(dataFrameConfig).getVisibleSignals();
        verify(dataFrameGenerateRequest).getInputDataFrameType();
    }
}
