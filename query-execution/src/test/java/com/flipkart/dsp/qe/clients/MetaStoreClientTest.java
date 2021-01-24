package com.flipkart.dsp.qe.clients;

import com.flipkart.dsp.qe.entity.HiveTableDetails;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MetaStoreClientTest {
    @Mock
    HiveMetaStoreClient hiveMetaStoreClient;

    @InjectMocks
    private MetaStoreClient metaStoreClient;

    private Table table;
    private StorageDescriptor sd;
    private SerDeInfo serdeInfo;
    private Map<String, String> parameters;
    private List<FieldSchema> cols;
    private List<FieldSchema> partitionKeys;
    private FieldSchema fieldSchema;
    private String db_tableName = "gouri.india";
    private String dbName = "gouri";
    private String tableName = "india";

    @Before
    public void setUp() throws TException {
        MockitoAnnotations.initMocks(this);
        table = new Table();
        sd = new StorageDescriptor();
        serdeInfo = new SerDeInfo();
        parameters = new HashMap<>();
        cols = new ArrayList<>();
        partitionKeys = new ArrayList<>();
        fieldSchema = new FieldSchema();
        table.setDbName("gouri");
        table.setTableName(tableName);
        table.setOwner("gouri.shankar");
        table.setCreateTime(1569408102);
        table.setLastAccessTime(0);
        table.setRetention(0);
        sd.setLocation("hdfs://hadoopcluster2/apps/hive/warehouse/ketan.db/india");
        sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
        sd.setCompressed(false);
        sd.setNumBuckets(-1);
        serdeInfo.setName(null);
        serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        parameters.put("serialization.format", ",");
        parameters.put("field.delim", ",");
        serdeInfo.setParameters(parameters);
        serdeInfo.setNameIsSet(true);
        sd.setSerdeInfo(serdeInfo);
        cols.add(new FieldSchema("office_name", "string", null));
        cols.add(new FieldSchema("office_status", "string", null));
        cols.add(new FieldSchema("pincode", "int", null));
        cols.add(new FieldSchema("telephone", "bigint", null));
        cols.add(new FieldSchema("taluk", "string", null));
        cols.add(new FieldSchema("district", "string", null));
        cols.add(new FieldSchema("postal_division", "string", null));
        cols.add(new FieldSchema("postal_region", "string", null));
        cols.add(new FieldSchema("postal_circle", "string", null));
        sd.setCols(cols);
        sd.setStoredAsSubDirectories(false);
        table.setSd(sd);
        for (int i = 1; i <= 7; i++)
            partitionKeys.add(new FieldSchema("par" + Integer.toString(i), "string", null));
        table.setPartitionKeys(partitionKeys);
        table.setTableType("MANAGED_TABLE");
        table.setViewOriginalText(null);
        table.setViewExpandedText(null);
        parameters.clear();
        System.out.println(serdeInfo.toString());
        parameters.put("transient_lastDdlTime", "1569408102");
        parameters.put("EXTERNAL","TRUE");
        table.setParameters(parameters);
        when(hiveMetaStoreClient.getTable(dbName, tableName)).thenReturn(table);
        when(hiveMetaStoreClient.listPartitionNames("gouri","india",(short) -1)).thenReturn(Lists.newArrayList("par1=1/par2=2/par3=3/par4=4/par5=5/par6=6/par7=7"));
    }

    @Test
    public void tableNotFoundTest() throws TException {
        try{
            metaStoreClient.getHiveTableFieldDelimit("gourishankar");
            assertTrue(false);
        }catch (TableNotFoundException t){
            assertTrue(true);
        }
    }

    @Test
    public void getHiveTableFieldDelimitTest() throws TException, TableNotFoundException {
        assertEquals(",", metaStoreClient.getHiveTableFieldDelimit(db_tableName));
        verify(hiveMetaStoreClient).getTable(dbName, tableName);
        verifyNoMoreInteractions(hiveMetaStoreClient);
    }

    @Test
    public void getColumnNamesTest() throws TException, TableNotFoundException {
        ArrayList<String> columnNames = metaStoreClient.getColumnNames(db_tableName);
        assertEquals(Arrays.asList("office_name", "office_status", "pincode", "telephone", "taluk", "district", "postal_division", "postal_region", "postal_circle", "par1", "par2", "par3", "par4", "par5", "par6", "par7"), columnNames);
        verify(hiveMetaStoreClient).getTable(dbName, tableName);
        verifyNoMoreInteractions(hiveMetaStoreClient);
    }

    @Test
    public void getPartitionedColumnNamesTest() throws TException, TableNotFoundException {
        Set<String> partitionedColumnNames = metaStoreClient.getPartitionedColumnNames(db_tableName);
        Set<String> expected = new HashSet<>(Arrays.asList("par1", "par2", "par3", "par4", "par5", "par6", "par7"));
        assertEquals(expected, partitionedColumnNames);
        verify(hiveMetaStoreClient).getTable(dbName, tableName);
        verifyNoMoreInteractions(hiveMetaStoreClient);
    }

    @Test
    public void getTableLocationTest() throws TException, TableNotFoundException {
        assertEquals("hdfs://hadoopcluster2/apps/hive/warehouse/ketan.db/india", metaStoreClient.getTableLocation(db_tableName));
        verify(hiveMetaStoreClient).getTable(dbName, tableName);
        verifyNoMoreInteractions(hiveMetaStoreClient);
    }

    @Test
    public void getHiveTableStorageFormat() throws TException, TableNotFoundException {
        assertEquals("'org.apache.hadoop.mapred.TextInputFormat'", metaStoreClient.getHiveTableStorageFormat(db_tableName));
        verify(hiveMetaStoreClient).getTable(dbName, tableName);
        verifyNoMoreInteractions(hiveMetaStoreClient);
    }

    @Test
    public void getColumnsWithTypeTest() throws TException, TableNotFoundException {
        LinkedHashMap<String, String> columnsWithType = metaStoreClient.getColumnsWithType(db_tableName);
        LinkedHashMap<String, String> expected = new LinkedHashMap<String, String>() {{
            put("office_name", "string");put("office_status", "string");put("pincode", "int");put("telephone", "bigint");put("taluk", "string");
            put("district", "string");put("postal_division", "string");put("postal_region", "string");put("postal_circle", "string");
            put("par1", "string");put("par2", "string");put("par3", "string");put("par4", "string");put("par5", "string");put("par6", "string");put("par7", "string");
        }};
        assertEquals(expected, columnsWithType);
        verify(hiveMetaStoreClient).getTable(dbName, tableName);
        verifyNoMoreInteractions(hiveMetaStoreClient);
    }

    @Test
    public void getHiveTableDetailsTest() throws TException, TableNotFoundException {
        HiveTableDetails hiveTableDetails = metaStoreClient.getHiveTableDetails(db_tableName);
        assertEquals(table.getDbName(), hiveTableDetails.getDatabase());
        assertEquals(table.getTableName(), hiveTableDetails.getTableName());
        assertEquals(table.getSd().getLocation(), hiveTableDetails.getLocation());
        assertEquals(true,hiveTableDetails.isExternal());
        verify(hiveMetaStoreClient).getTable(dbName, tableName);

    }

    @Test(expected = TException.class)
    public void getColumnsWithTypeTableTExceptionTest() throws TException, TableNotFoundException {
        doThrow(new TException())
                .when(hiveMetaStoreClient).getTable(dbName, tableName);
        metaStoreClient.getColumnsWithType(db_tableName);
        verify(hiveMetaStoreClient).getTable(dbName, tableName);
    }

    @Test
    public void getHiveTableFieldDelimitWhenfield_delimIsMissingTest() throws TException, TableNotFoundException {
        parameters.clear();
        parameters.put("separatorChar", ",");
        serdeInfo.setParameters(parameters);
        assertEquals(",", metaStoreClient.getHiveTableFieldDelimit(db_tableName));
        verify(hiveMetaStoreClient).getTable(dbName, tableName);
        verifyNoMoreInteractions(hiveMetaStoreClient);
    }

    @Test
    public void getHiveTableFieldDelimitWhenNoDelimTest() throws TException, TableNotFoundException {
        parameters.clear();
        serdeInfo.setParameters(parameters);
        assertEquals("\\001", metaStoreClient.getHiveTableFieldDelimit(db_tableName));
        verify(hiveMetaStoreClient).getTable(dbName, tableName);
        verifyNoMoreInteractions(hiveMetaStoreClient);
    }
    @Test
    public void checkPartitionExistsTest() throws TException, TableNotFoundException {
        assertFalse(metaStoreClient.checkPartitionExists(db_tableName,"par1","2"));
        assertFalse(metaStoreClient.checkPartitionExists(db_tableName,"par8","2"));
        assertTrue(metaStoreClient.checkPartitionExists(db_tableName,"par1","1"));
    }
}
