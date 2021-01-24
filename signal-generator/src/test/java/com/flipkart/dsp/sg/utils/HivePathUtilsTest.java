package com.flipkart.dsp.sg.utils;

import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.sg.exceptions.HiveGeneratorException;
import com.flipkart.dsp.sg.hiveql.base.Table;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.flipkart.dsp.utils.Constants.dot;
import static java.lang.String.format;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * +
 */
public class HivePathUtilsTest {
    @Mock private Table table;
    @Mock private MetaStoreClient metaStoreClient;

    private Long requestId = 1L;
    private String dbName = "test_db";
    private HivePathUtils hivePathUtils;
    private String tableName = "test_table";
    private Map<String, Long> tables = new HashMap<>();
    private String fullName = dbName + dot + tableName;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        this.hivePathUtils = spy(new HivePathUtils(metaStoreClient));

        when(table.getDbName()).thenReturn(dbName);
        when(table.getTableName()).thenReturn(tableName);
    }

    @Test
    public void testGetHDFSPathFromHiveTableSuccess() throws Exception {
        when(metaStoreClient.getTableLocation(anyString())).thenReturn("location");
        String expected = hivePathUtils.getHDFSPathFromHiveTable(requestId, table);
        assertNotNull(expected);
        assertEquals(expected, "location");
        verify(table).getDbName();
        verify(table).getTableName();
        verify(metaStoreClient).getTableLocation(anyString());
    }

    @Test
    public void testGetHDFSPathFromHiveTableFailure() throws Exception {
        boolean isException = false;
        when(metaStoreClient.getTableLocation(anyString())).thenThrow(new TableNotFoundException(tableName, "Error"));

        try {
            hivePathUtils.getHDFSPathFromHiveTable(requestId, table);
        } catch (HiveGeneratorException e) {
            isException = true;
            String message = format("Could not build path for UseCase Payload. Request ID : %s DB Name : %s Table Name : %s", requestId, dbName, tableName);
            assertEquals(e.getMessage(), message);
        }

        assertTrue(isException);
        verify(table).getDbName();
        verify(table).getTableName();
        verify(metaStoreClient).getTableLocation(anyString());
    }

    @Test
    public void testGetPartitionPathSuccess() {
        tables.put(fullName, 1L);
        String expected = hivePathUtils.getPartitionPath("path", table, tables);
        assertNotNull(expected);
        assertEquals(expected, "path/refresh_id=1");
        verify(table).getDbName();
        verify(table).getTableName();
    }


    @Test
    public void testGetPartitionPathFailure() {
        boolean isException = false;
        try {
            hivePathUtils.getPartitionPath("path", table, tables);
        } catch (NoSuchElementException e) {
            isException = true;
            assertTrue(e.getMessage().contains( "No refresh-id found for table : " + dbName + "."));
        }

        assertTrue(isException);
        verify(table, times(2)).getDbName();
        verify(table).getTableName();
    }

}
