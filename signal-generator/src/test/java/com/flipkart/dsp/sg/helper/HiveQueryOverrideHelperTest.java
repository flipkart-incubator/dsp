package com.flipkart.dsp.sg.helper;

import com.flipkart.dsp.config.MiscConfig;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.overrides.HiveQueryDataframeOverride;
import com.flipkart.dsp.models.sg.SignalDataType;
import com.flipkart.dsp.qe.clients.HiveClient;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.sg.exceptions.DataframeOverrideException;
import com.flipkart.dsp.utils.Encryption;
import com.flipkart.dsp.utils.HdfsUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static com.flipkart.dsp.utils.Constants.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * +
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Encryption.class})
public class HiveQueryOverrideHelperTest {
    @Mock private HdfsUtils hdfsUtils;
    @Mock private HiveClient hiveClient;
    @Mock private MetaStoreClient metaStoreClient;
    @Mock private MiscConfig miscConfig;
    @Mock private DataFrameOverrideHelper dataFrameOverrideHelper;
    @Mock private HiveQueryDataframeOverride hiveQueryDataframeOverride;

    private String saltKey = "saltKey";
    private String dataBaseName = "test_db";
    private String tableName = "test_table";
    private String dataFrameName = "dataFrameName";
    private String hive_query = "select * from test_table";
    private String locationOnHDFS = "/projects/planning/test_db.db/test_table/category=Iron/refresh_id=1";

    private List<String> files = new ArrayList<>();
    private HiveQueryOverrideHelper hiveQueryOverrideHelper;
    private LinkedHashMap<String, SignalDataType> columnMapping = new LinkedHashMap<>();


    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(Encryption.class);
        MockitoAnnotations.initMocks(this);
        this.hiveQueryOverrideHelper = spy(new HiveQueryOverrideHelper(hdfsUtils, hiveClient, metaStoreClient, miscConfig, dataFrameOverrideHelper));

        files.add(locationOnHDFS);
        columnMapping.put("column1", SignalDataType.TEXT);
        columnMapping.put("column2", SignalDataType.DOUBLE);

        doNothing().when(hiveClient).setQueue(anyString());
        doNothing().when(hiveClient).executeQuery(anyString());
        when(miscConfig.getSaltKey()).thenReturn(saltKey);
        when(hiveQueryDataframeOverride.getQuery()).thenReturn(hive_query);
        when(hdfsUtils.getAllFilesUnderDirectory(any())).thenReturn(files);
        when(hiveQueryDataframeOverride.getColumnMapping()).thenReturn(columnMapping);
    }

    @Test
    public void testGetOverrideHash() throws Exception {
        String overrideHash = "overrideHash";
        PowerMockito.when(Encryption.encrypt(anyString(), eq(saltKey))).thenReturn(overrideHash);

        String expected = hiveQueryOverrideHelper.getOverrideHash(hiveQueryDataframeOverride);
        assertNotNull(expected);
        assertEquals(expected, overrideHash);
        verify(miscConfig).getSaltKey();
        verify(hiveQueryDataframeOverride).getQuery();
        verify(hiveQueryDataframeOverride).getColumnMapping();
        PowerMockito.verifyStatic(Encryption.class, times(3));
        Encryption.encrypt(anyString(), eq(saltKey));

    }

    @Test
    public void testGetCreateColumnQueryForHiveQuery() {
        String expected = hiveQueryOverrideHelper.getCreateColumnQueryForHiveQuery(hiveQueryDataframeOverride);
        assertEquals(expected, "column1 string,column2 double");
        verify(hiveQueryDataframeOverride).getColumnMapping();
    }

    @Test
    public void testExecuteQuerySuccess() throws Exception {
        Set<LinkedHashMap<String, String>> partitionMapSet = new HashSet<>();
        String content = "string_value1\u00011.00\nstring_value2\u00011.20";

        when(hdfsUtils.getFolderSize(locationOnHDFS)).thenReturn(10L);
        Map<String, Long> expected = hiveQueryOverrideHelper.executeQuery(dataBaseName, tableName, PRODUCTION_HIVE_QUEUE, dataFrameName, hiveQueryDataframeOverride);
        assertNotNull(expected);
        assertEquals(expected.size(), 1);
        assertTrue(expected.containsKey(HIVE_QUERY_DATABASE + dot + tableName));

        verify(hiveQueryDataframeOverride).getQuery();
        verify(hiveClient).executeQuery(anyString());
        verify(hdfsUtils).getAllFilesUnderDirectory(any());
        verify(hdfsUtils).getFolderSize(locationOnHDFS);
    }

    @Test
    public void testExecuteQueryFailureCase1() throws Exception {
        boolean isException = false;
        doThrow(new HiveClientException("Error")).when(hiveClient).executeQuery(anyString());

        try {
            hiveQueryOverrideHelper.executeQuery(dataBaseName, tableName, PRODUCTION_HIVE_QUEUE, dataFrameName, hiveQueryDataframeOverride);
        } catch (DataframeOverrideException e) {
            isException = true;
            assertEquals(e.getMessage(), "Exception while executing hive query.\nquery: " + hive_query + "\n" + "errorMessage: Error");
        }

        assertTrue(isException);
        verify(hiveQueryDataframeOverride).getQuery();
        verify(hiveClient).executeQuery(anyString());
    }

    @Test
    public void testExecuteQueryFailureCase2() throws Exception {
        boolean isException = false;
        files.clear();
        when(hdfsUtils.getAllFilesUnderDirectory(any())).thenReturn(files);

        try {
            hiveQueryOverrideHelper.executeQuery(dataBaseName, tableName, PRODUCTION_HIVE_QUEUE, dataFrameName, hiveQueryDataframeOverride);
        } catch (ValidationException e) {
            isException = true;
            assertEquals(e.getMessage(), "No Output Files found for hiveQuery DataFrame: " + dataFrameName);
        }

        assertTrue(isException);
        verify(hiveQueryDataframeOverride).getQuery();
        verify(hiveClient).executeQuery(anyString());
        verify(hdfsUtils).getAllFilesUnderDirectory(any());
    }

    @Test
    public void testExecuteQueryFailureCase3() throws Exception {
        boolean isException = false;
        String content = "";

        when(hdfsUtils.getFolderSize(locationOnHDFS)).thenReturn(0L);

        try {
            hiveQueryOverrideHelper.executeQuery(dataBaseName, tableName, PRODUCTION_HIVE_QUEUE, dataFrameName, hiveQueryDataframeOverride);
        } catch (ValidationException e) {
            isException = true;
            assertTrue(e.getMessage().contains("Output Data can't be empty for hiveQuery DataFrame: " + dataFrameName));
        }

        assertTrue(isException);
        verify(hiveQueryDataframeOverride).getQuery();
        verify(hiveClient).executeQuery(anyString());
        verify(hdfsUtils).getAllFilesUnderDirectory(any());
        verify(hdfsUtils).getFolderSize(locationOnHDFS);
    }

}
