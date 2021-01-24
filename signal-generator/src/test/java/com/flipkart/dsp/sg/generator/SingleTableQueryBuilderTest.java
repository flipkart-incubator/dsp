package com.flipkart.dsp.sg.generator;

import com.flipkart.dsp.config.HiveConfig;
import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.models.overrides.DataframeOverride;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.sg.exceptions.HiveGeneratorException;
import com.flipkart.dsp.sg.helper.ConstraintHelper;
import com.flipkart.dsp.sg.helper.PartitionHelper;
import com.flipkart.dsp.sg.hiveql.base.Constraint;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.query.CreateQuery;
import com.flipkart.dsp.sg.hiveql.query.InsertQuery;
import com.flipkart.dsp.sg.exceptions.InvalidQueryException;
import com.flipkart.dsp.sg.override.OverrideManager;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static com.flipkart.dsp.utils.Constants.dot;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.verifyNew;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CreateQuery.class, InsertQuery.class, SingleTableQueryBuilder.class})
public class SingleTableQueryBuilderTest {

    @Mock private Table table;
    @Mock private Signal signal;
    @Mock private Signal signal1;
    @Mock private DataTable dataTable;
    @Mock private DataFrame dataframe;
    @Mock private HiveConfig hiveConfig;
    @Mock private Constraint constraint;
    @Mock private DataSource dataSource;
    @Mock private SignalGroup signalGroup;
    @Mock private CreateQuery createQuery;
    @Mock private InsertQuery insertQuery;
    @Mock private DataFrameScope dataFrameScope;
    @Mock private PartitionHelper partitionHelper;
    @Mock private OverrideManager overrideManager;
    @Mock private DataFrameConfig dataFrameConfig;
    @Mock private ConstraintHelper constraintHelper;
    @Mock private SignalGroup.SignalMeta signalMeta;
    @Mock private DataFrameBuilder dataFrameBuilder;
    @Mock private SignalDefinition signalDefinition;
    @Mock private DataframeOverride dataframeOverride;
    @Mock private DataSourceConfiguration dataSourceConfiguration;
    @Mock private DataFrameGenerateRequest dataFrameGenerateRequest;

    private SingleTableQueryBuilder singleTableQueryBuilder;
    private Set<Constraint> constraints = new HashSet<>();
    private Map<Table, Long> tableRefreshID = new HashMap<>();
    private LinkedHashSet<Signal> signals = new LinkedHashSet<>();
    private Map<String, Long> tableRefreshIDMap = new HashMap<>();
    private Set<DataFrameScope> dataFrameScopeSet = new HashSet<>();
    private List<SignalGroup.SignalMeta> signalMetas = new ArrayList<>();
    private LinkedHashSet<Table> upstreamDataTables = new LinkedHashSet<>();
    private Map<String, DataframeOverride> dataframeOverrideMap = new HashMap<>();
    private Map<String, Pair<String, SignalDefinition>> signalDefinitionMap = new HashMap<>();
    private LinkedHashMap<DataTable, LinkedHashSet<Signal>> tableToSignalMap = new LinkedHashMap<>();

    private Long runId = 1L;
    private String dbName = "test_db";
    private String tableName = "test_table";
    private String fullTableName = dbName + dot + tableName;
    private String dataFrameName = "dataFrameName";
    private String signalBaseEntity = "signalBaseEntity";


    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(CreateQuery.class);
        PowerMockito.mockStatic(InsertQuery.class);
        MockitoAnnotations.initMocks(this);
        this.singleTableQueryBuilder = spy(new SingleTableQueryBuilder(hiveConfig, partitionHelper, overrideManager, constraintHelper, dataFrameBuilder));

        signals.add(signal);
        signalMetas.add(signalMeta);
        constraints.add(constraint);
        upstreamDataTables.add(table);
        tableRefreshID.put(table, 1L);
        dataFrameScopeSet.add(dataFrameScope);
        tableRefreshIDMap.put(fullTableName, 1L);
        tableToSignalMap.put(dataTable, signals);
        dataframeOverrideMap.put(dataFrameName, dataframeOverride);
        signalDefinitionMap.put("", Pair.with("", signalDefinition));

        when(dataframe.getId()).thenReturn(1L);
        when(dataframe.getName()).thenReturn(dataFrameName);
        when(dataframe.getSignalGroup()).thenReturn(signalGroup);
        when(dataframe.getDataFrameConfig()).thenReturn(dataFrameConfig);
        when(dataFrameConfig.getVisibleSignals()).thenReturn(signals);

        when(dataTable.getId()).thenReturn(tableName);
        when(dataTable.getDataSource()).thenReturn(dataSource);
        when(dataSourceConfiguration.getDatabase()).thenReturn(dbName);
        when(dataSource.getConfiguration()).thenReturn(dataSourceConfiguration);

        when(signal.getSignalBaseEntity()).thenReturn(signalBaseEntity);
        when(signal.getSignalDataType()).thenReturn(SignalDataType.TEXT);
        when(signal.getSignalDefinition()).thenReturn(signalDefinition);
        when(signalMeta.getSignal()).thenReturn(signal);
        when(signalMeta.getDataTable()).thenReturn(dataTable);
        when(signalDefinition.getDefaultValue()).thenReturn(1);
        when(signalGroup.getSignalMetas()).thenReturn(signalMetas);

        when(dataFrameGenerateRequest.getTables()).thenReturn(tableRefreshIDMap);
        when(dataFrameGenerateRequest.getDataFrameOverrideMap()).thenReturn(dataframeOverrideMap);

        when(dataFrameBuilder.getUpstreamDataTables(tableToSignalMap)).thenReturn(upstreamDataTables);
        when(dataFrameBuilder.getSignalDefinitionMap(tableToSignalMap)).thenReturn(signalDefinitionMap);
        when(dataFrameBuilder.getDataTableToSignalMap(dataframe, false)).thenReturn(tableToSignalMap);
        when(dataFrameBuilder.getTableToRefreshId(dataFrameGenerateRequest, upstreamDataTables)).thenReturn(tableRefreshID);

        when(hiveConfig.getSgDatabase()).thenReturn(dbName);
        when(createQuery.constructQuery()).thenReturn("createQuery");
        when(insertQuery.constructQuery()).thenReturn("insertQuery");
        when(partitionHelper.getPartitionForDataframe(dataframe)).thenReturn(signals);
        PowerMockito.whenNew(CreateQuery.class).withAnyArguments().thenReturn(createQuery);
        PowerMockito.whenNew(InsertQuery.class).withAnyArguments().thenReturn(insertQuery);
        when(overrideManager.getDataTableForOverride(dataframe, dataframeOverride, dataTable)).thenReturn(dataTable);
        when(constraintHelper.buildConstraintSet(signalDefinitionMap, dataFrameScopeSet, tableRefreshID, dataTable, dataframe)).thenReturn(constraints);
    }

    // NO Query
    @Test
    public void testBuildQuerySuccessCase1() throws Exception {
        when(dataframe.getSgType()).thenReturn(SGType.NO_QUERY);
        singleTableQueryBuilder.buildQuery(runId, dataframe, dataFrameGenerateRequest, dataFrameScopeSet);

        verify(hiveConfig).getSgDatabase();
        verify(dataFrameGenerateRequest, times(3)).getDataFrameOverrideMap();
        verify(dataframe, times(3)).getName();
        verify(dataframe, times(6)).getSignalGroup();
        verify(signalGroup, times(6)).getSignalMetas();
        verify(signalMeta, times(2)).getDataTable();
        verify(overrideManager).getDataTableForOverride(dataframe, dataframeOverride, dataTable);
        verify(dataframe).getDataFrameConfig();
        verify(dataFrameConfig).getVisibleSignals();
        verify(partitionHelper).getPartitionForDataframe(dataframe);
        verify(signalMeta, times(1)).getSignal();
        verify(signal, times(1)).getSignalBaseEntity();
        verify(signal, times(1)).getSignalDataType();
        verify(signal, times(1)).getSignalDefinition();
        verify(signalDefinition, times(1)).getDefaultValue();
        verify(dataTable).getDataSource();
        verify(dataSource).getConfiguration();
        verify(dataSourceConfiguration).getDatabase();
        verify(dataTable).getId();
        verify(dataframe).getSgType();
        verify(dataFrameGenerateRequest).getTables();
    }

    // NO Query
    @Test
    public void testBuildQuerySuccessCase2() throws Exception {
        tableRefreshIDMap.clear();
        when(dataframe.getSgType()).thenReturn(SGType.NO_QUERY);
        singleTableQueryBuilder.buildQuery(runId, dataframe, dataFrameGenerateRequest, dataFrameScopeSet);

        verify(hiveConfig).getSgDatabase();
        verify(dataFrameGenerateRequest, times(3)).getDataFrameOverrideMap();
        verify(dataframe, times(3)).getName();
        verify(dataframe, times(6)).getSignalGroup();
        verify(signalGroup, times(6)).getSignalMetas();
        verify(signalMeta, times(2)).getDataTable();
        verify(overrideManager).getDataTableForOverride(dataframe, dataframeOverride, dataTable);
        verify(dataframe).getDataFrameConfig();
        verify(dataFrameConfig).getVisibleSignals();
        verify(partitionHelper).getPartitionForDataframe(dataframe);
        verify(signalMeta, times(1)).getSignal();
        verify(signal, times(1)).getSignalBaseEntity();
        verify(signal, times(1)).getSignalDataType();
        verify(signal, times(1)).getSignalDefinition();
        verify(signalDefinition, times(1)).getDefaultValue();
        verify(dataTable).getDataSource();
        verify(dataSource).getConfiguration();
        verify(dataSourceConfiguration).getDatabase();
        verify(dataTable).getId();
        verify(dataframe).getSgType();
        verify(dataFrameGenerateRequest).getTables();
    }

    // SINGLE_TABLE_QUERY
    @Test
    public void testBuildQuerySuccessCase3() throws Exception {
        when(dataframe.getSgType()).thenReturn(SGType.SINGLE_TABLE_QUERY);
        singleTableQueryBuilder.buildQuery(runId, dataframe, dataFrameGenerateRequest, dataFrameScopeSet);

        verify(hiveConfig).getSgDatabase();
        verify(dataFrameGenerateRequest, times(3)).getDataFrameOverrideMap();
        verify(dataframe, times(4)).getName();
        verify(dataframe, times(7)).getSignalGroup();
        verify(signalGroup, times(7)).getSignalMetas();
        verify(signalMeta, times(3)).getDataTable();
        verify(overrideManager).getDataTableForOverride(dataframe, dataframeOverride, dataTable);
        verify(dataframe).getDataFrameConfig();
        verify(dataFrameConfig).getVisibleSignals();
        verify(partitionHelper).getPartitionForDataframe(dataframe);
        verify(signalMeta, times(1)).getSignal();
        verify(signal, times(1)).getSignalBaseEntity();
        verify(signal, times(1)).getSignalDataType();
        verify(signal, times(1)).getSignalDefinition();
        verify(signalDefinition, times(1)).getDefaultValue();
        verify(dataTable).getDataSource();
        verify(dataSource).getConfiguration();
        verify(dataSourceConfiguration).getDatabase();
        verify(dataTable).getId();
        verify(dataframe).getSgType();
        verify(dataFrameBuilder).getDataTableToSignalMap(dataframe, false);
        verify(dataFrameBuilder).getSignalDefinitionMap(tableToSignalMap);
        verify(dataFrameBuilder).getUpstreamDataTables(tableToSignalMap);
        verify(dataFrameBuilder).getTableToRefreshId(dataFrameGenerateRequest, upstreamDataTables);
        verify(constraintHelper).buildConstraintSet(signalDefinitionMap, dataFrameScopeSet, tableRefreshID, dataTable, dataframe);
        verify(dataframe).getId();
        verifyNew(CreateQuery.class).withArguments(any());
        verify(createQuery).constructQuery();
        verifyNew(InsertQuery.class).withArguments(any(), any(), any());
        verify(insertQuery).constructQuery();
    }

    // NO Query
    @Test
    public void testBuildQueryFailureCase1() throws Exception {
        signals.add(signal1);
        when(dataframe.getName()).thenReturn(signalBaseEntity);
        when(dataframe.getSgType()).thenReturn(SGType.NO_QUERY);
        when(dataFrameConfig.getVisibleSignals()).thenReturn(signals);

        boolean isException = false;
        try {
            singleTableQueryBuilder.buildQuery(runId, dataframe, dataFrameGenerateRequest, dataFrameScopeSet);
        } catch (HiveGeneratorException e) {
            isException = true;
            assertTrue(e.getMessage().contains("Number of signal in visible signal can't be greater then no of signals in signal group "));
        }

        assertTrue(isException);
        verify(hiveConfig).getSgDatabase();
        verify(dataFrameGenerateRequest, times(2)).getDataFrameOverrideMap();
        verify(dataframe, times(2)).getName();
        verify(dataframe, times(3)).getSignalGroup();
        verify(signalGroup, times(3)).getSignalMetas();
        verify(signalMeta, times(1)).getDataTable();
    }

    // SINGLE_TABLE_QUERY
    @Test
    public void testBuildQueryFailureCase2() throws Exception {
        when(dataframe.getTableName()).thenReturn(tableName);
        when(insertQuery.constructQuery()).thenThrow(new InvalidQueryException("Error"));
        when(dataframe.getSgType()).thenReturn(SGType.SINGLE_TABLE_QUERY);

        boolean isException = false;
        try {
            singleTableQueryBuilder.buildQuery(runId, dataframe, dataFrameGenerateRequest, dataFrameScopeSet);
        } catch (HiveGeneratorException e) {
            isException = true;
            assertEquals(e.getMessage(), "Failed to create insert query for the dataFrame table : "+ tableName);;
        }

        assertTrue(isException);
        verify(hiveConfig).getSgDatabase();
        verify(dataFrameGenerateRequest, times(3)).getDataFrameOverrideMap();
        verify(dataframe, times(4)).getName();
        verify(dataframe, times(6)).getSignalGroup();
        verify(signalGroup, times(6)).getSignalMetas();
        verify(signalMeta, times(3)).getDataTable();
        verify(overrideManager).getDataTableForOverride(dataframe, dataframeOverride, dataTable);
        verify(dataframe).getDataFrameConfig();
        verify(dataFrameConfig).getVisibleSignals();
        verify(partitionHelper).getPartitionForDataframe(dataframe);
        verify(signalMeta, times(1)).getSignal();
        verify(signal, times(1)).getSignalBaseEntity();
        verify(signal, times(1)).getSignalDataType();
        verify(signal, times(1)).getSignalDefinition();
        verify(signalDefinition, times(1)).getDefaultValue();
        verify(dataTable).getDataSource();
        verify(dataSource).getConfiguration();
        verify(dataSourceConfiguration).getDatabase();
        verify(dataTable).getId();
        verify(dataframe).getSgType();
        verify(dataFrameBuilder).getDataTableToSignalMap(dataframe, false);
        verify(dataFrameBuilder).getSignalDefinitionMap(tableToSignalMap);
        verify(dataFrameBuilder).getUpstreamDataTables(tableToSignalMap);
        verify(dataFrameBuilder).getTableToRefreshId(dataFrameGenerateRequest, upstreamDataTables);
        verify(constraintHelper).buildConstraintSet(signalDefinitionMap, dataFrameScopeSet, tableRefreshID, dataTable, dataframe);
        verify(dataframe).getId();
        verifyNew(CreateQuery.class).withArguments(any());
        verify(createQuery).constructQuery();
        verifyNew(InsertQuery.class).withArguments(any(), any(), any());
        verify(insertQuery).constructQuery();
        verify(dataframe).getTableName();
    }

}
