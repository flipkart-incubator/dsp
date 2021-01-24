package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.dto.DataFrameGenerateRequest;
import com.flipkart.dsp.entities.sg.dto.DataFrameKey;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import com.flipkart.dsp.exceptions.HDFSUtilsException;
import com.flipkart.dsp.models.DataType;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.sg.exceptions.DataFrameGeneratorException;
import com.flipkart.dsp.sg.exceptions.HiveGeneratorException;
import com.flipkart.dsp.sg.hiveql.base.Column;
import com.flipkart.dsp.sg.hiveql.core.HiveColumn;
import com.flipkart.dsp.sg.hiveql.core.HiveTable;
import com.flipkart.dsp.sg.utils.GeneratorUtils;
import com.flipkart.dsp.sg.utils.HivePathUtils;
import com.flipkart.dsp.utils.HdfsUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URI;
import java.util.*;

import static com.flipkart.dsp.utils.Constants.comma;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SGUseCasePayloadBuilder.class, GeneratorUtils.class})
public class SGUseCasePayloadBuilderTest {
    @Mock private Path path;
    @Mock private Signal signal;
    @Mock private HiveColumn column;
    @Mock private DataFrame dataFrame;
    @Mock private HdfsUtils hdfsUtils;
    @Mock private FileStatus fileStatus;
    @Mock private DataFrameKey dataFrameKey;
    @Mock private HivePathUtils hivePathUtils;
    @Mock private DataFrameScope dataFrameScope;
    @Mock private AbstractPredicateClause abstractPredicateClause;
    @Mock private DataFrameGenerateRequest dataFrameGenerateRequest;

    private URI uri;
    private HiveTable hiveTable;
    private String columnName = "column1";
    private String signalName = "signal1";
    private String dataFrameName = "dataFrameName";
    private SGUseCasePayloadBuilder sgUseCasePayloadBuilder;

    private Map<String, Long> tables = new HashMap<>();
    private FileStatus[] fileStatuses = new FileStatus[1];
    private LinkedList<Column> columns = new LinkedList<>();
    private Set<DataFrameScope> dataFrameScopes = new HashSet<>();
    private Map<String, DataType> inputDataframeType = new HashMap<>();
    private LinkedHashSet<Column> partitionColumns = new LinkedHashSet<>();
    private Triplet<Object, Pair<Object, Object>, Set<Object>> triplet = Triplet.with(0, new Pair<>(0,0), new HashSet<>());

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(GeneratorUtils.class);
        MockitoAnnotations.initMocks(this);
        this.sgUseCasePayloadBuilder = spy(new SGUseCasePayloadBuilder(hdfsUtils, hivePathUtils));

        uri = new URI("column1=1");
        fileStatuses[0] = fileStatus;
        columns.add(column);
        partitionColumns.add(column);
        dataFrameScopes.add(dataFrameScope);
        hiveTable = HiveTable.builder().db("test_db").name("test_table").columns(columns).delimiter(comma).build();

        when(dataFrameGenerateRequest.getRequestId()).thenReturn(1L);
        when(dataFrameGenerateRequest.getTables()).thenReturn(tables);
        when(dataFrameGenerateRequest.getInputDataFrameType()).thenReturn(inputDataframeType);

        when(column.isPartitionColumn()).thenReturn(true);
        when(column.getColumnName()).thenReturn(columnName);

        when(signal.getName()).thenReturn(signalName);
        when(dataFrameScope.getSignal()).thenReturn(signal);
        when(dataFrame.getName()).thenReturn(dataFrameName);
        when(dataFrame.getSgType()).thenReturn(SGType.NO_QUERY);

        when(path.toUri()).thenReturn(uri);
        when(path.getParent()).thenReturn(path);
        when(fileStatus.getPath()).thenReturn(path);
        when(hdfsUtils.getFilesUnderDirectory(any())).thenReturn(fileStatuses);
        when(abstractPredicateClause.getPredicateType()).thenReturn(PredicateType.NOT_IN);
        when(dataFrameScope.getAbstractPredicateClause()).thenReturn(abstractPredicateClause);
        when(hivePathUtils.getHDFSPathFromHiveTable(1L, hiveTable)).thenReturn("path");
        when(hivePathUtils.getPartitionPath("path", hiveTable, tables)).thenReturn("partitionPath");
        PowerMockito.when(GeneratorUtils.convertAbstractPredicateClauseToDataFrameKey(abstractPredicateClause)).thenReturn(dataFrameKey);
        PowerMockito.when(GeneratorUtils.getPartitionKeys(hiveTable)).thenReturn(partitionColumns);
    }

    @Test
    public void testBuildSuccessCase1() throws Exception {
        inputDataframeType.put(dataFrameName, DataType.DATAFRAME);
        when(abstractPredicateClause.getPredicateType()).thenReturn(PredicateType.IN);
        PowerMockito.when(GeneratorUtils.getPartitionKeys(hiveTable)).thenReturn(partitionColumns);

        SGUseCasePayload expected =  sgUseCasePayloadBuilder.build(dataFrameGenerateRequest, hiveTable, dataFrameScopes, dataFrame);
        assertNotNull(expected);
        verify(dataFrameGenerateRequest).getRequestId();
        verify(dataFrameGenerateRequest).getTables();
        verify(dataFrameGenerateRequest).getInputDataFrameType();
        verify(column).isPartitionColumn();
        verify(column, times(2)).getColumnName();
        verify(dataFrameScope, times(4)).getSignal();
        verify(signal, times(4)).getName();
        verify(dataFrameScope, times(2)).getAbstractPredicateClause();
        verify(abstractPredicateClause).getPredicateType();
        verify(hivePathUtils).getHDFSPathFromHiveTable(1L, hiveTable);
        verify(hivePathUtils).getPartitionPath("path", hiveTable, tables);
        verify(hdfsUtils).getFilesUnderDirectory(any());
        verify(fileStatus, times(2)).getPath();
        verify(path).toUri();
        PowerMockito.verifyStatic(GeneratorUtils.class);
        GeneratorUtils.convertAbstractPredicateClauseToDataFrameKey(abstractPredicateClause);
        verify(path).getParent();
    }

    @Test
    public void testBuildFSuccessCase2() throws Exception {
        inputDataframeType.put(dataFrameName, DataType.DATAFRAME_PATH);
        when(abstractPredicateClause.getPredicateType()).thenReturn(PredicateType.IN);
        PowerMockito.when(GeneratorUtils.getPartitionKeys(hiveTable)).thenReturn(partitionColumns);

        SGUseCasePayload expected =  sgUseCasePayloadBuilder.build(dataFrameGenerateRequest, hiveTable, dataFrameScopes, dataFrame);
        assertNotNull(expected);
        verify(dataFrameGenerateRequest).getRequestId();
        verify(dataFrameGenerateRequest).getTables();
        verify(dataFrameGenerateRequest).getInputDataFrameType();
        verify(column).isPartitionColumn();
        verify(column, times(2)).getColumnName();
        verify(dataFrameScope, times(4)).getSignal();
        verify(signal, times(4)).getName();
        verify(dataFrameScope, times(2)).getAbstractPredicateClause();
        verify(abstractPredicateClause).getPredicateType();
        verify(hivePathUtils).getHDFSPathFromHiveTable(1L, hiveTable);
        verify(hivePathUtils).getPartitionPath("path", hiveTable, tables);
        verify(hdfsUtils).getFilesUnderDirectory(any());
        verify(fileStatus, times(2)).getPath();
        verify(path).toUri();
        PowerMockito.verifyStatic(GeneratorUtils.class);
        GeneratorUtils.convertAbstractPredicateClauseToDataFrameKey(abstractPredicateClause);
    }

    @Test
    public void testBuildFailureCase1() throws Exception {
        boolean isException = false;
        try {
            sgUseCasePayloadBuilder.build(dataFrameGenerateRequest, hiveTable, dataFrameScopes, dataFrame);
        } catch (HiveGeneratorException e) {
            isException = true;
            assertEquals(e.getMessage(), "Failed to convert PredicateDataType : NOT_IN to DataFrameColumnType");
        }

        assertTrue(isException);
        verify(dataFrameGenerateRequest).getRequestId();
        verify(dataFrameGenerateRequest).getTables();
        verify(dataFrameGenerateRequest).getInputDataFrameType();
        verify(column).isPartitionColumn();
        verify(column).getColumnName();
        verify(dataFrameScope, times(2)).getSignal();
        verify(signal, times(2)).getName();
        verify(dataFrameScope).getAbstractPredicateClause();
        verify(abstractPredicateClause).getPredicateType();
    }

    @Test
    public void testBuildFailureCase2() throws Exception {
        uri = new URI("column1=__HIVE_DEFAULT_PARTITION__");
        inputDataframeType.put(dataFrameName, DataType.DATAFRAME);

        when(path.toUri()).thenReturn(uri);
        when(abstractPredicateClause.getPredicateType()).thenReturn(PredicateType.IN);

        boolean isException = false;
        try {
            sgUseCasePayloadBuilder.build(dataFrameGenerateRequest, hiveTable, dataFrameScopes, dataFrame);
        } catch (DataFrameGeneratorException e) {
            isException = true;
            assertEquals(e.getMessage(), "No real dataframe generated for Dataframe : " + dataFrameName + " because there is no data in it.");
        }

        assertTrue(isException);
        verify(dataFrameGenerateRequest).getRequestId();
        verify(dataFrameGenerateRequest).getTables();
        verify(dataFrameGenerateRequest).getInputDataFrameType();
        verify(column).isPartitionColumn();
        verify(column, times(2)).getColumnName();
        verify(dataFrameScope, times(2)).getSignal();
        verify(signal, times(2)).getName();
        verify(dataFrameScope, times(1)).getAbstractPredicateClause();
        verify(abstractPredicateClause).getPredicateType();
        verify(hivePathUtils).getHDFSPathFromHiveTable(1L, hiveTable);
        verify(hivePathUtils).getPartitionPath("path", hiveTable, tables);
        verify(hdfsUtils).getFilesUnderDirectory(any());
        verify(fileStatus, times(1)).getPath();
        verify(path).toUri();
        verify(dataFrame).getName();
        PowerMockito.verifyStatic(GeneratorUtils.class);
        GeneratorUtils.getPartitionKeys(hiveTable);
    }

    @Test
    public void testBuildFailureCase3() throws Exception {
        inputDataframeType.put(dataFrameName, DataType.DATAFRAME_PATH);
        when(abstractPredicateClause.getPredicateType()).thenReturn(PredicateType.IN);
        when(hdfsUtils.getFilesUnderDirectory(any())).thenThrow(new HDFSUtilsException("Error"));

        boolean isException = false;
        try {
            sgUseCasePayloadBuilder.build(dataFrameGenerateRequest, hiveTable, dataFrameScopes, dataFrame);
        } catch (DataFrameGeneratorException e) {
            isException = true;
            assertEquals(e.getMessage(), "No real dataframe generated for Dataframe : " + dataFrameName);
        }
        assertTrue(isException);

        verify(dataFrameGenerateRequest).getRequestId();
        verify(dataFrameGenerateRequest).getTables();
        verify(dataFrameGenerateRequest).getInputDataFrameType();
        verify(column).isPartitionColumn();
        verify(column, times(2)).getColumnName();
        verify(dataFrameScope, times(2)).getSignal();
        verify(signal, times(2)).getName();
        verify(dataFrameScope).getAbstractPredicateClause();
        verify(abstractPredicateClause).getPredicateType();
        verify(hivePathUtils).getHDFSPathFromHiveTable(1L, hiveTable);
        verify(hivePathUtils).getPartitionPath("path", hiveTable, tables);
        verify(hdfsUtils).getFilesUnderDirectory(any());
    }

    @Test
    public void testBuildFailureCase4() throws Exception {
        FileStatus[] fileStatuses = new FileStatus[2];
        fileStatuses[0] = fileStatus;
        fileStatuses[1] = fileStatus;
        inputDataframeType.put(dataFrameName, DataType.DATAFRAME_PATH);
        when(abstractPredicateClause.getPredicateType()).thenReturn(PredicateType.IN);
        when(hdfsUtils.getFilesUnderDirectory(any())).thenReturn(fileStatuses);


        boolean isException = false;
        try {
            sgUseCasePayloadBuilder.build(dataFrameGenerateRequest, hiveTable, dataFrameScopes, dataFrame);
        } catch (DataFrameGeneratorException e) {
            isException = true;
            assertTrue(e.getMessage().contains("hdfs paths for dataframe " + dataFrameName + " have multiple files!."));
        }
        assertTrue(isException);

        verify(dataFrameGenerateRequest).getRequestId();
        verify(dataFrameGenerateRequest).getTables();
        verify(dataFrameGenerateRequest).getInputDataFrameType();
        verify(column).isPartitionColumn();
        verify(column, times(2)).getColumnName();
        verify(dataFrameScope, times(6)).getSignal();
        verify(signal, times(6)).getName();
        verify(dataFrameScope, times(3)).getAbstractPredicateClause();
        verify(abstractPredicateClause).getPredicateType();
        verify(hivePathUtils).getHDFSPathFromHiveTable(1L, hiveTable);
        verify(hivePathUtils).getPartitionPath("path", hiveTable, tables);
        verify(hdfsUtils).getFilesUnderDirectory(any());
        verify(fileStatus, times(3)).getPath();
        verify(path, times(2)).toUri();
        PowerMockito.verifyStatic(GeneratorUtils.class, times(2));
        GeneratorUtils.convertAbstractPredicateClauseToDataFrameKey(abstractPredicateClause);
    }



}
