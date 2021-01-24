package com.flipkart.dsp.qe.entity;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 */
public class HiveTableDetailsTest {

    private String tableName = "test_temp";
    private String databaseName = "dsp_output";
    private String fullTableName = databaseName + "." + tableName;
    private String createTableIfNotExist = "CREATE TABLE IF NOT EXISTS ";
    private String columnString = "(`column1` string,\t `column2` string," +
            " `column3` struct <c1:string,c2:float,c3:float,c4:int>, " +
            " `column4` ARRAY<ARRAY<double>>, `column5` string  )";
    private String partitionedString = " PARTITIONED BY (`refresh_id` bigint,`target_sc` string)";
    private HiveTableDetails hiveTableDetails = new HiveTableDetails();

    @Test
    public void testSerialize() {
        HiveTableDetails.TableColumn column = HiveTableDetails.TableColumn.builder()
                .columnName("column1").columnType("double").build();
        HiveTableDetails.TableColumn partitionedColumn = HiveTableDetails.TableColumn.builder()
                .columnName("partitionedColumn").columnType("double").build();
        List<HiveTableDetails.TableColumn> columns = new ArrayList<>();
        List<HiveTableDetails.TableColumn> partitionedColumns = new ArrayList<>();

        columns.add(column);
        partitionedColumns.add(partitionedColumn);
        hiveTableDetails.setDatabase(databaseName);
        hiveTableDetails.setTableName(tableName);
        hiveTableDetails.setColumns(columns);
        hiveTableDetails.setPartitionedColumns(partitionedColumns);

        String createString = hiveTableDetails.serialize();
        System.out.println(createString);
    }

    // Column Test
    @Test
    public void testDeSerializeCase1() throws Exception {
        String createTable = "CREATE TABLE ";
        String createString = createTable + tableName + columnString + ";";
        HiveTableDetails expected = new HiveTableDetails(createString);
        assertEquals(expected.getTableName(), tableName);
        assertNotNull(expected.getDatabase());
    }

    // Partitioned Column Test
    @Test
    public void testDeSerializeCase2() throws Exception {
        String createString = createTableIfNotExist + fullTableName + columnString + partitionedString + ";";
        HiveTableDetails expected = new HiveTableDetails(createString);
        assertNotNull(expected.getDatabase());
        assertEquals(expected.getDatabase(), databaseName);
        assertEquals(expected.getPartitionedColumns().size(), 2);
        assertEquals(expected.getColumns().size(), 5);
    }

    // Location Test
    @Test
    public void testDeSerializeCase3() throws Exception {
        String locationString = " LOCATION 'hdfs://hadoopcluster2/projects/planning/dcp_fact.db/test_temp'";
        String createString = createTableIfNotExist + fullTableName + columnString
                + partitionedString + locationString + ";";
        HiveTableDetails expected = new HiveTableDetails(createString);
        assertTrue(expected.getLocation().contains("hdfs://hadoopcluster2/projects/planning/dcp_fact.db/test_temp"));
    }


    // StoredAsFileTest
    @Test
    public void testDeSerializeCase4() throws Exception {
        String storedAsFileString = " STORED AS TEXTFILE.";
        String createString = createTableIfNotExist + fullTableName + columnString
                + partitionedString + storedAsFileString + ";";
        HiveTableDetails expected = new HiveTableDetails(createString);
        assertTrue(expected.getStoredAsBlock().contains("TEXTFILE"));
    }

    // StoredAsFormatTest
    @Test
    public void testDeSerializeCase5() throws Exception {
        String storedAsFormatString = " STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'"
                + " OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'";
        String createString = createTableIfNotExist + fullTableName + columnString
                + partitionedString + storedAsFormatString + ";";
        HiveTableDetails expected = new HiveTableDetails(createString);
        assertTrue(expected.getStoredAsBlock().contains("INPUTFORMAT"));
        assertTrue(expected.getStoredAsBlock().contains("org.apache.hadoop.mapred.TextInputFormat"));
        assertTrue(expected.getStoredAsBlock().contains("OUTPUTFORMAT"));
        assertTrue(expected.getStoredAsBlock().contains("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"));
    }

    // StoredByTest
    @Test
    public void testDeSerializeCase6() throws Exception {
        String storedByAsSerdeString = " STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
                + " WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \"cf:string\"," +
                "\"hbase.table.name\" = \"hbase_table_0\")";
        String createString = createTableIfNotExist + fullTableName + columnString
                + partitionedString + storedByAsSerdeString + ";";
        HiveTableDetails expected = new HiveTableDetails(createString);
        assertTrue(expected.getStoredByBlock().contains("org.apache.hadoop.hive.hbase.HBaseStorageHandler"));
    }

    // TablePropertiesTest
    @Test
    public void testDeSerializeCase7() throws Exception {
        String tablePropertiesString = " TBLPROPERTIES ('transient_lastDdlTime'='1554366469')";
        String createString = createTableIfNotExist + fullTableName + columnString
                + partitionedString + tablePropertiesString + ";";
        HiveTableDetails expected = new HiveTableDetails(createString);
        assertTrue(expected.getTableProperties().contains("transient_lastDdlTime"));
    }

    // RowFormat SerDe without properties Test
    @Test
    public void testDeSerializeCase8() throws Exception {
        String rowFormatSerDeString = " ROW FORMAT SERDE"
                + " 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'";
        String createString = createTableIfNotExist + fullTableName + columnString
                + partitionedString + rowFormatSerDeString + ";";
        HiveTableDetails expected = new HiveTableDetails(createString);
        assertTrue(expected.getRowFormat().contains("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"));
        assertFalse(expected.getRowFormat().contains("WITH SERDEPROPERTIES"));
    }

    // RowFormat SerDe with properties Test
    @Test
    public void testDeSerializeCase9() throws Exception {
        String rowFormatSerDeWithPropertiesString = " ROW FORMAT SERDE"
                + " 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'"
                + " WITH SERDEPROPERTIES ('field.delim'=',','line.delim'='\\n',"
                + "'serialization.format'=',')";
        String createString = createTableIfNotExist + fullTableName + columnString
                + partitionedString + rowFormatSerDeWithPropertiesString + ";";
        HiveTableDetails expected = new HiveTableDetails(createString);
        assertTrue(expected.getRowFormat().contains("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"));
        assertTrue(expected.getRowFormat().contains("WITH SERDEPROPERTIES"));
        assertTrue(expected.getRowFormat().contains("field.delim"));
    }

    // RowFormat without SerDe Test
    @Test
    public void testDeSerializeCase10() throws Exception {
        String rowFormatWithoutSerDe = "ROW FORMAT DELIMITED " +
                "FIELDS TERMINATED BY '\t' ESCAPED BY '\"' LINES TERMINATED BY '\n' ";
        String createString = createTableIfNotExist + fullTableName + columnString
                + partitionedString + rowFormatWithoutSerDe + ";";
        HiveTableDetails expected = new HiveTableDetails(createString);
        assertTrue(expected.getRowFormat().contains("FIELDS TERMINATED BY '\t'"));
    }

    @Test
    public void testDeSerializeCase1Failure() throws Exception {
        String createString = createTableIfNotExist + fullTableName + columnString
                + partitionedString + "stored by ";

        boolean isException = false;
        try {
            new HiveTableDetails(createString);
        } catch (Exception e) {
            isException = true;
            assertEquals(e.getMessage(), "Invalid or Incomplete stored by details for hive table while deserialization");
        }
        assertTrue(isException);
    }

    @Test
    public void testDeSerializeCase2Failure() throws Exception {
        String createString = createTableIfNotExist + fullTableName + columnString
                + partitionedString + "stored as ";

        boolean isException = false;
        try {
            new HiveTableDetails(createString);
        } catch (Exception e) {
            isException = true;
            assertEquals(e.getMessage(), "Invalid or Incomplete stored as details for hive table while deserialization");
        }
        assertTrue(isException);
    }

    @Test
    public void testDeSerializeCase3Failure() throws Exception {
        String createString = createTableIfNotExist + fullTableName + columnString
                + partitionedString + " TBLPROPERTIES ";

        boolean isException = false;
        try {
            new HiveTableDetails(createString);
        } catch (Exception e) {
            isException = true;
            assertEquals(e.getMessage(), "Invalid or Incomplete table properties details for hive table while deserialization");
        }
        assertTrue(isException);
    }

    @Test
    public void testTableColumnEqualsCase1() {
        HiveTableDetails.TableColumn column1 = HiveTableDetails.TableColumn.builder().build();
        HiveTableDetails.TableColumn column2 = HiveTableDetails.TableColumn.builder().build();

        assertEquals(column1, column2);
        assertEquals(column1, column1);
    }

    @Test
    public void testTableColumnEqualsCase2() {
        HiveTableDetails.TableColumn column1 = HiveTableDetails.TableColumn.builder()
                .columnName("column1").build();
        HiveTableDetails.TableColumn column2 = HiveTableDetails.TableColumn.builder()
                .columnName("column1").build();
        assertEquals(column1, column2);

        column1.setColumnType("double");
        assertNotEquals(column1, column2);

        column2.setColumnType("string");
        assertNotEquals(column1, column2);

        column2.setColumnType("double");
        assertEquals(column1, column2);
    }

}
