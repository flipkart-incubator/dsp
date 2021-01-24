package com.flipkart.dsp.qe.clients;

import com.flipkart.dsp.qe.entity.HiveConfigParam;
import com.flipkart.dsp.qe.entity.HiveTableDetails;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({HiveClient.class, DriverManager.class})
public class HiveClientTest {
    @Mock ResultSet resultSet;
    @Mock Statement statement;
    @Mock Connection connection;
    private HiveClient hiveClient;
    @Mock private HiveConfigParam hiveConfigParam;
    private String tableName = "dsp_output.test_table";


    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        PowerMockito.mockStatic(DriverManager.class);
        hiveClient = spy(new HiveClient(hiveConfigParam));

        when(hiveConfigParam.getUrl()).thenReturn("url");
        when(hiveConfigParam.getUser()).thenReturn("user");
        when(hiveConfigParam.getPassword()).thenReturn("password");
        PowerMockito.when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
    }

    @Test
    public void testGetHiveTableDetailsSuccess() throws Exception {
        HiveTableDetails output = new HiveTableDetails();
        output.setTableName(tableName);
        when(statement.executeQuery(anyString())).thenReturn(resultSet);
        PowerMockito.whenNew(HiveTableDetails.class).withAnyArguments().thenReturn(output);
        HiveTableDetails expected = hiveClient.getHiveTableDetails(tableName);
        assertEquals(expected.getTableName(), output.getTableName());
    }

    @Test
    public void testGetHiveTableDetailsFailure() throws Exception {
        boolean isException = false;
        when(statement.executeQuery(anyString())).thenThrow(new SQLException());

        try {
            hiveClient.getHiveTableDetails(tableName);
        } catch (HiveClientException e) {
            isException = true;
            assertTrue(e.getMessage().contains( "Error In Executing Query "));
        }

        assertTrue(isException);
        verify(hiveConfigParam, times(1)).getUrl();
        verify(hiveConfigParam, times(1)).getUser();
        verify(hiveConfigParam, times(1)).getPassword();
        verify(connection, times(1)).createStatement();
        verify(statement, times(1)).executeQuery(anyString());

    }
}
