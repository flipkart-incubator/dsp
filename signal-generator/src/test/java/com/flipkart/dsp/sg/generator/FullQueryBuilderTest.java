package com.flipkart.dsp.sg.generator;

import com.flipkart.dsp.config.HiveConfig;
import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.sg.exceptions.HiveGeneratorException;
import com.flipkart.dsp.sg.exceptions.InvalidQueryException;
import com.flipkart.dsp.sg.helper.ConstraintHelper;
import com.flipkart.dsp.sg.helper.PartitionHelper;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Constraint;
import com.flipkart.dsp.sg.hiveql.base.Table;
import com.flipkart.dsp.sg.hiveql.query.CreateQuery;
import com.flipkart.dsp.sg.hiveql.query.InsertQuery;
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
import static java.lang.String.format;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CreateQuery.class, InsertQuery.class, FullQueryBuilder.class})
public class FullQueryBuilderTest {
    @Mock private Table table;
    @Mock private Table table1;
    @Mock private Table table2;
    @Mock private Signal signal;
    @Mock private Column column;
    @Mock private DataFrame dataFrame;
    @Mock private DataTable dataTable;
    @Mock private Constraint constraint;
    @Mock private DataSource dataSource;
    @Mock private Signal partitionSignal;
    @Mock private HiveConfig hiveConfig;
    @Mock private CreateQuery createQuery;
    @Mock private InsertQuery insertQuery;
    @Mock private SignalGroup signalGroup;
    @Mock private PartitionHelper partitionHelper;
    @Mock private MetaStoreClient metaStoreClient;
    @Mock private DataFrameBuilder dataFrameBuilder;
    @Mock private ConstraintHelper constraintHelper;
    @Mock private SignalDefinition signalDefinition;
    @Mock private SignalGroup.SignalMeta signalMeta;
    @Mock private DataSourceConfiguration dataSourceConfiguration;
    @Mock private DataFrameGenerateRequest dataFrameGenerateRequest;

    private Long runId = 1L;
    private String dbName = "test_db";
    private String dataSourceId = dbName;
    private String tableName = "test_table";
    private String dataTableId = tableName;
    private String columnName = "signalName1";
    private String signalBaseEntity = columnName;
    private String fullTableName = dbName + dot + tableName;

    private FullQueryBuilder fullQueryBuilder;
    private Map<String, Long> tablesMap = new HashMap<>();
    private Set<Constraint> constraints = new HashSet<>();
    private LinkedList<Column> columns = new LinkedList<>();
    private ArrayList<String> columnNames = new ArrayList<>();
    private LinkedHashSet<Table> tables = new LinkedHashSet<>();
    private Map<Table, Long> tableToRefreshId = new HashMap<>();
    private LinkedHashSet<Signal> signals = new LinkedHashSet<>();
    private Set<DataFrameScope> dataFrameScopeSet = new HashSet<>();
    private LinkedHashSet<Signal> partitions = new LinkedHashSet<>();
    private List<SignalGroup.SignalMeta> signalMetas = new ArrayList<>();
    private Map<String, Pair<String, SignalDefinition>> signalDefinitions = new HashMap<>();
    private LinkedHashMap<DataTable, LinkedHashSet<Signal>> tableToSignals = new LinkedHashMap<>();
    private Pair<String, SignalDefinition> signalDefinitionPair = Pair.with("signal", signalDefinition);

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(CreateQuery.class);
        PowerMockito.mockStatic(InsertQuery.class);
        MockitoAnnotations.initMocks(this);
        this.fullQueryBuilder = spy(new FullQueryBuilder(hiveConfig, partitionHelper, metaStoreClient, dataFrameBuilder, constraintHelper));

        String signalName = "signalName";
        String dataFrameName = "dataFrameName";

        tables.add(table);
        signals.add(signal);
        columns.add(column);
        constraints.add(constraint);
        signalMetas.add(signalMeta);
        columnNames.add(columnName);
        partitions.add(partitionSignal);
        tableToRefreshId.put(table, 1L);
        tablesMap.put(fullTableName, 1L);
        tableToSignals.put(dataTable, signals);
        signalDefinitions.put(signalBaseEntity, signalDefinitionPair);

        when(signal.getName()).thenReturn(signalName);
        when(signal.getSignalBaseEntity()).thenReturn(signalBaseEntity);
        when(signal.getSignalDefinition()).thenReturn(signalDefinition);
        when(signal.getSignalDataType()).thenReturn(SignalDataType.TEXT);
        when(partitionSignal.getSignalDataType()).thenReturn(SignalDataType.TEXT);

        when(signalMeta.isPrimary()).thenReturn(true);
        when(signalMeta.getSignal()).thenReturn(signal);
        when(signalGroup.getSignalMetas()).thenReturn(signalMetas);
        when(signalDefinition.getAggregationType()).thenReturn(AggregationType.NA);
        when(signalDefinition.getGroupBy()).thenReturn(null);

        when(column.getColumnName()).thenReturn(columnName);
        when(dataTable.getId()).thenReturn(dataTableId);
        when(dataTable.getDataSource()).thenReturn(dataSource);

        when(dataSource.getId()).thenReturn(dataSourceId);
        when(dataSource.getConfiguration()).thenReturn(dataSourceConfiguration);
        when(dataSourceConfiguration.getDatabase()).thenReturn(dbName);

        when(dataFrame.getId()).thenReturn(1L);
        when(dataFrame.getName()).thenReturn(dataFrameName);
        when(dataFrame.getSignalGroup()).thenReturn(signalGroup);

        when(createQuery.constructQuery()).thenReturn("createQuery");
        PowerMockito.whenNew(CreateQuery.class).withAnyArguments().thenReturn(createQuery);
        PowerMockito.whenNew(InsertQuery.class).withAnyArguments().thenReturn(insertQuery);

        when(dataFrameBuilder.getUpstreamDataTables(tableToSignals)).thenReturn(tables);
        when(dataFrameBuilder.getSignalDefinitionMap(tableToSignals)).thenReturn(signalDefinitions);
        when(dataFrameBuilder.constructColumns(tableToSignals, new ArrayList<>())).thenReturn(columns);
        when(dataFrameBuilder.getDataTableToSignalMap(dataFrame, true)).thenReturn(tableToSignals);
        when(dataFrameBuilder.getDataTableToSignalMap(dataFrame, false)).thenReturn(tableToSignals);
        when(dataFrameBuilder.getTableToRefreshId(dataFrameGenerateRequest, tables)).thenReturn(tableToRefreshId);

        when(hiveConfig.getSgDatabase()).thenReturn(dbName);
        when(dataFrameGenerateRequest.getTables()).thenReturn(tablesMap);
        when(partitionHelper.getPartitionForDataframe(dataFrame)).thenReturn(partitions);
        when(constraintHelper.buildConstraintSet(any(), any(), any(), any(), any())).thenReturn(constraints);
        when(metaStoreClient.getColumnNames(dbName + dot + tableName)).thenReturn(columnNames);
    }

    @Test
    public void testBuildQuerySuccess() throws Exception {
        tables.add(table1);
        when(dataFrameBuilder.getUpstreamDataTables(tableToSignals)).thenReturn(tables);
        when(insertQuery.constructQuery()).thenReturn("insertQuery");

        Pair<Table, List<String>> expected = fullQueryBuilder.buildQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopeSet);
        assertNotNull(expected);
        verify(dataFrame, times(3)).getName();
        verify(dataFrameBuilder, times(2)).getDataTableToSignalMap(dataFrame, true);
        verify(hiveConfig, times(3)).getSgDatabase();
        verify(dataFrameBuilder).constructColumns(tableToSignals, new ArrayList<>());
        verify(dataFrameBuilder).getUpstreamDataTables(tableToSignals);
        verify(dataFrameBuilder).getTableToRefreshId(dataFrameGenerateRequest, tables);
        verify(signal, times(8)).getSignalBaseEntity();
        verify(signal, times(8)).getName();
        verify(dataFrameBuilder).getSignalDefinitionMap(tableToSignals);
        verify(dataFrameBuilder).getDataTableToSignalMap(dataFrame, false);
        verify(signal, times(6)).getSignalDefinition();
        verify(signalDefinition).getGroupBy();
        verify(dataFrame, times(2)).getSignalGroup();
        verify(signalGroup, times(2)).getSignalMetas();
        verify(signalMeta).isPrimary();
        verify(dataTable, times(2)).getDataSource();
        verify(dataSource).getId();
        verify(partitionSignal, times(1)).getSignalDataType();
        verify(signal, times(2)).getSignalDataType();
        verify(dataTable, times(4)).getId();
        verify(signalDefinition).getAggregationType();
        verify(signalDefinition, times(3)).getDefaultValue();
        verify(dataSource).getConfiguration();
        verify(dataSourceConfiguration).getDatabase();
        verify(dataFrameGenerateRequest, times(2)).getTables();
        verify(constraintHelper, times(2)).buildConstraintSet(any(), any(), any(), any(), any());
        verify(partitionHelper).getPartitionForDataframe(dataFrame);
        verify(dataFrame).getId();
        PowerMockito.verifyNew(CreateQuery.class).withArguments(any());
        PowerMockito.verifyNew(InsertQuery.class).withArguments(any(), any(), any());
    }

    @Test
    public void testBuildQueryFailureCase1() throws Exception {
        when(signalDefinition.getGroupBy()).thenReturn(new HashSet<>());
        boolean isException = false;
        try {
            fullQueryBuilder.buildQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopeSet);
        } catch (RuntimeException e) {
            isException = true;
            String errorMessage = format("Fact table %s.%s doesn't have any of the granularity columns", dbName, tableName);
            assertEquals(e.getMessage(), "Exception while generating intermediate fact tables");
            assertEquals(e.getCause().getMessage(), errorMessage);
        }

        assertTrue(isException);
        verify(dataFrame, times(2)).getName();
        verify(dataFrameBuilder, times(2)).getDataTableToSignalMap(dataFrame, true);
        verify(hiveConfig, times(2)).getSgDatabase();
        verify(dataFrameBuilder).constructColumns(tableToSignals, new ArrayList<>());
        verify(dataFrameBuilder).getUpstreamDataTables(tableToSignals);
        verify(dataFrameBuilder).getTableToRefreshId(dataFrameGenerateRequest, tables);
        verify(signal, times(2)).getSignalBaseEntity();
        verify(signal, times(4)).getName();
        verify(dataFrameBuilder).getSignalDefinitionMap(tableToSignals);
        verify(constraintHelper).buildConstraintSet(signalDefinitions, dataFrameScopeSet, tableToRefreshId, null, dataFrame);
        verify(dataFrameBuilder).getDataTableToSignalMap(dataFrame, false);
        verify(signal).getSignalDefinition();
        verify(signalDefinition).getGroupBy();
        verify(dataFrame).getSignalGroup();
        verify(signalGroup).getSignalMetas();
        verify(signalMeta).isPrimary();
        verify(signal).getSignalDataType();
        verify(dataTable, times(2)).getDataSource();
        verify(dataTable, times(3)).getId();
        verify(dataSource).getId();
        verify(dataSource).getConfiguration();
        verify(dataSourceConfiguration).getDatabase();
    }

    @Test
    public void testBuildQueryFailureCase2() throws Exception {
        when(metaStoreClient.getColumnNames(dbName + dot + tableName)).thenThrow(new TableNotFoundException(tableName, "Error"));

        boolean isException = false;
        try {
            fullQueryBuilder.buildQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopeSet);
        } catch (RuntimeException e) {
            isException = true;
            String errorMessage = format("Failed to get column names for table: %s.%s", dbName, tableName);
            assertEquals(e.getMessage(), "Exception while generating intermediate fact tables");
            assertEquals(e.getCause().getMessage(), errorMessage);
        }

        assertTrue(isException);
        verify(dataFrame, times(2)).getName();
        verify(dataFrameBuilder, times(2)).getDataTableToSignalMap(dataFrame, true);
        verify(hiveConfig, times(2)).getSgDatabase();
        verify(dataFrameBuilder).constructColumns(tableToSignals, new ArrayList<>());
        verify(dataFrameBuilder).getUpstreamDataTables(tableToSignals);
        verify(dataFrameBuilder).getTableToRefreshId(dataFrameGenerateRequest, tables);
        verify(signal, times(2)).getSignalBaseEntity();
        verify(signal, times(4)).getName();
        verify(dataFrameBuilder).getSignalDefinitionMap(tableToSignals);
        verify(constraintHelper).buildConstraintSet(signalDefinitions, dataFrameScopeSet, tableToRefreshId, null, dataFrame);
        verify(dataFrameBuilder).getDataTableToSignalMap(dataFrame, false);
        verify(signal).getSignalDefinition();
        verify(signalDefinition).getGroupBy();
        verify(dataFrame).getSignalGroup();
        verify(signalGroup).getSignalMetas();
        verify(signalMeta).isPrimary();
        verify(signal).getSignalDataType();
        verify(dataTable).getDataSource();
        verify(dataTable, times(2)).getId();
        verify(dataSource).getId();
    }

    @Test
    public void testBuildQueryFailureCase3() throws Exception {
        when(dataFrameGenerateRequest.getTables()).thenReturn(null);
        boolean isException = false;
        try {
            fullQueryBuilder.buildQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopeSet);
        } catch (RuntimeException e) {
            isException = true;
            assertEquals(e.getMessage(), "Cannot create Table with empty tables list");
        }

        assertTrue(isException);
        verify(dataFrame, times(2)).getName();
        verify(dataFrameBuilder, times(2)).getDataTableToSignalMap(dataFrame, true);
        verify(hiveConfig, times(2)).getSgDatabase();
        verify(dataFrameBuilder).constructColumns(tableToSignals, new ArrayList<>());
        verify(dataFrameBuilder).getUpstreamDataTables(tableToSignals);
        verify(dataFrameBuilder).getTableToRefreshId(dataFrameGenerateRequest, tables);
        verify(signal, times(7)).getSignalBaseEntity();
        verify(signal, times(7)).getName();
        verify(dataFrameBuilder).getSignalDefinitionMap(tableToSignals);
        verify(constraintHelper).buildConstraintSet(signalDefinitions, dataFrameScopeSet, tableToRefreshId, null, dataFrame);
        verify(dataFrameBuilder).getDataTableToSignalMap(dataFrame, false);
        verify(signal, times(6)).getSignalDefinition();
        verify(signalDefinition).getGroupBy();
        verify(dataFrame).getSignalGroup();
        verify(signalGroup).getSignalMetas();
        verify(signalMeta).isPrimary();
        verify(dataTable, times(2)).getDataSource();
        verify(dataSource).getId();
        verify(signal, times(2)).getSignalDataType();
        verify(dataTable, times(3)).getId();
        verify(signalDefinition).getAggregationType();
        verify(signalDefinition, times(3)).getDefaultValue();
        verify(dataSource).getConfiguration();
        verify(dataSourceConfiguration).getDatabase();
        verify(dataFrameGenerateRequest).getTables();
    }

    @Test
    public void testBuildQueryFailureCase4() throws Exception {
        tables.add(table1);
        tables.add(table2);
        when(insertQuery.constructQuery()).thenThrow(new InvalidQueryException("Error"));

        boolean isException = false;
        try {
            fullQueryBuilder.buildQuery(runId, dataFrame, dataFrameGenerateRequest, dataFrameScopeSet);
        } catch (HiveGeneratorException e) {
            isException = true;
            String errorMessage = format("Failed to create insert query for the dataframe table : %s", dataFrame.getTableName());
            assertEquals(e.getMessage(), errorMessage);
        }

        assertTrue(isException);
        verify(dataFrame, times(3)).getName();
        verify(dataFrameBuilder, times(2)).getDataTableToSignalMap(dataFrame, true);
        verify(hiveConfig, times(3)).getSgDatabase();
        verify(dataFrameBuilder).constructColumns(tableToSignals, new ArrayList<>());
        verify(dataFrameBuilder).getUpstreamDataTables(tableToSignals);
        verify(dataFrameBuilder).getTableToRefreshId(dataFrameGenerateRequest, tables);
        verify(signal, times(8)).getSignalBaseEntity();
        verify(signal, times(8)).getName();
        verify(dataFrameBuilder).getSignalDefinitionMap(tableToSignals);
        verify(dataFrameBuilder).getDataTableToSignalMap(dataFrame, false);
        verify(signal, times(6)).getSignalDefinition();
        verify(signalDefinition).getGroupBy();
        verify(dataFrame, times(2)).getSignalGroup();
        verify(signalGroup, times(2)).getSignalMetas();
        verify(signalMeta).isPrimary();
        verify(dataTable, times(2)).getDataSource();
        verify(dataSource).getId();
        verify(signal, times(2)).getSignalDataType();
        verify(dataTable, times(4)).getId();
        verify(signalDefinition).getAggregationType();
        verify(signalDefinition, times(3)).getDefaultValue();
        verify(dataSource).getConfiguration();
        verify(dataSourceConfiguration).getDatabase();
        verify(dataFrameGenerateRequest, times(2)).getTables();
        verify(constraintHelper, times(2)).buildConstraintSet(any(), any(), any(), any(), any());
        verify(partitionHelper).getPartitionForDataframe(dataFrame);
        verify(dataFrame).getId();
        PowerMockito.verifyNew(CreateQuery.class).withArguments(any());
        PowerMockito.verifyNew(InsertQuery.class).withArguments(any(), any(), any());
    }
}
