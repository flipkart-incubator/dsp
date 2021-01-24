package com.flipkart.dsp.sg.generator;

import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.sg.exceptions.DataFrameGeneratorException;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.base.Table;
import org.javatuples.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;

import static com.flipkart.dsp.utils.Constants.dot;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class DataFrameBuilderTest {
    @Mock private Table table;
    @Mock private Signal signal;
    @Mock private DataTable dataTable;
    @Mock private DataFrame dataFrame;
    @Mock private DataSource dataSource;
    @Mock private SignalGroup signalGroup;
    @Mock private SignalDefinition signalDefinition;
    @Mock private SignalGroup.SignalMeta signalMeta;
    @Mock private DataSourceConfiguration dataSourceConfiguration;
    @Mock private DataFrameGenerateRequest dataFrameGenerateRequest;

    private DataFrameBuilder dataFrameBuilder;

    private String database = "test_db";
    private String tableName = "test_table";
    private String dataTableId = "dataTableId";
    private String signalBaseEntity = "baseEntity";
    private String fullTableName = database + dot + tableName;
    private LinkedHashSet<Signal> signals = new LinkedHashSet<>();
    private LinkedHashSet<Table> upstreamDataTables = new LinkedHashSet<>();
    private List<SignalGroup.SignalMeta> signalMetaList = new ArrayList<>();
    private LinkedHashMap<DataTable, LinkedHashSet<Signal>> tableToSignalMap = new LinkedHashMap<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.dataFrameBuilder = spy(new DataFrameBuilder());

        signals.add(signal);
        signalMetaList.add(signalMeta);
        upstreamDataTables.add(table);
        tableToSignalMap.put(dataTable, signals);
        when(dataTable.getId()).thenReturn(dataTableId);
        when(signal.getSignalBaseEntity()).thenReturn(signalBaseEntity);
        when(signal.getSignalDataType()).thenReturn(SignalDataType.TEXT);
    }

    @Test
    public void testGetDataTableToSignalMap() {
        when(dataFrame.getSignalGroup()).thenReturn(signalGroup);
        when(signalGroup.getSignalMetas()).thenReturn(signalMetaList);
        when(signalMeta.isPrimary()).thenReturn(true);
        when(signalMeta.getDataTable()).thenReturn(dataTable);
        when(signalMeta.getSignal()).thenReturn(signal);

        LinkedHashMap<DataTable, LinkedHashSet<Signal>> expected = dataFrameBuilder.getDataTableToSignalMap(dataFrame, true);
        assertNotNull(expected);
        assertTrue(expected.containsKey(dataTable));
        assertEquals(expected.get(dataTable).size(), 1);
        verify(dataFrame, times(1)).getSignalGroup();
        verify(signalGroup, times(1)).getSignalMetas();
        verify(signalMeta, times(1)).isPrimary();
        verify(signalMeta, times(1)).getDataTable();
        verify(signalMeta, times(1)).getSignal();
    }

    @Test
    public void testGetSignalDefinitionMap() {
        when(signal.getSignalDefinition()).thenReturn(signalDefinition);

        Map<String, Pair<String, SignalDefinition>> expected = dataFrameBuilder.getSignalDefinitionMap(tableToSignalMap);
        assertNotNull(expected);
        assertTrue(expected.containsKey(signalBaseEntity));
        assertEquals(expected.get(signalBaseEntity).getValue0(), dataTableId);
        assertEquals(expected.get(signalBaseEntity).getValue1(), signalDefinition);
        verify(dataTable, times(1)).getId();
        verify(signal, times(1)).getSignalBaseEntity();
        verify(signal, times(1)).getSignalDefinition();
    }

    @Test
    public void testConstructColumns() {
        String signalName = "signalName";
        List<String> partitions = new ArrayList<>();
        partitions.add(signalName);
        when(signal.getName()).thenReturn(signalName);

        LinkedList<Column> columns = dataFrameBuilder.constructColumns(tableToSignalMap, partitions);
        assertNotNull(columns);
        assertEquals(columns.size(), 1);
        assertEquals(columns.get(0).getColumnName(), signalName);
        assertTrue(columns.get(0).isPartitionColumn());
        verify(signal, times(2)).getName();
        verify(signal, times(1)).getSignalDataType();
    }

    @Test
    public void testGetUpstreamDataTables() {
        when(dataTable.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConfiguration()).thenReturn(dataSourceConfiguration);
        when(dataSourceConfiguration.getDatabase()).thenReturn(database);
        LinkedHashSet<Table> tables = dataFrameBuilder.getUpstreamDataTables(tableToSignalMap);
        assertNotNull(tables);
        assertEquals(tables.size(), 1);
        verify(signal, times(1)).getSignalBaseEntity();
        verify(signal, times(1)).getSignalDataType();
        verify(dataTable, times(1)).getDataSource();
        verify(dataSource, times(1)).getConfiguration();
        verify(dataSourceConfiguration, times(1)).getDatabase();
        verify(dataTable, times(1)).getId();
    }

    @Test
    public void testGetTableToRefreshIdSuccess() throws Exception {
        Map<String, Long> tables = new HashMap<>();
        tables.put(fullTableName, 123L);

        when(table.getDbName()).thenReturn(database);
        when(table.getTableName()).thenReturn(tableName);
        when(dataFrameGenerateRequest.getTables()).thenReturn(tables);

        Map<Table, Long> expected = dataFrameBuilder.getTableToRefreshId(dataFrameGenerateRequest, upstreamDataTables);
        assertNotNull(expected);
        assertEquals(expected.size(), 1);
        assertTrue(expected.containsKey(table));
        assertEquals(expected.get(table).longValue(), 123L);
        verify(table, times(1)).getDbName();
        verify(table, times(1)).getTableName();
        verify(dataFrameGenerateRequest, times(2)).getTables();
    }

    @Test
    public void testGetTableToRefreshIdFailure() throws Exception {
        Map<String, Long> tables = new HashMap<>();
        when(dataFrameGenerateRequest.getTables()).thenReturn(tables);

        boolean isException = false;
        try {
            dataFrameBuilder.getTableToRefreshId(dataFrameGenerateRequest, upstreamDataTables);
        } catch (DataFrameGeneratorException e) {
            isException = true;
            assertEquals(e.getMessage(), "Table To RefreshId Map shouldn't be empty");
        }
        assertTrue(isException);
        verify(dataFrameGenerateRequest, times(1)).getTables();
    }
}
