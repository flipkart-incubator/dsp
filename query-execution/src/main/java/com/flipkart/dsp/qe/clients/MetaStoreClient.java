package com.flipkart.dsp.qe.clients;

import com.flipkart.dsp.qe.entity.HiveTableDetails;
import com.flipkart.dsp.qe.entity.HiveTableDetails.TableColumn;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.qe.utils.RetryWaitLogic;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import javafx.util.Pair;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;

import java.util.*;


@Slf4j
@Singleton
public class MetaStoreClient {

    HiveMetaStoreClient hiveMetaStoreClient;
    private static Integer retryGapInMillis = 1000;
    private static Integer maxRetries = 3;

    @Inject
    public MetaStoreClient(HiveMetaStoreClient hiveMetaStoreClient) throws MetaException {
        log.info("HiveMetaStoreClient Initialized ThreadId {} ", Thread.currentThread());
        this.hiveMetaStoreClient = hiveMetaStoreClient;
    }

    public ArrayList<String> getColumnNames(String tableName) throws TException, TableNotFoundException {
        Pair<String, String> pair = getDbTableName(tableName);
        ArrayList<String> columnNames = new ArrayList<>();
        int retryGapInMillisecond = retryGapInMillis;
        for (int i = 0; i < maxRetries; i++) {
            try {
                Table table = hiveMetaStoreClient.getTable(pair.getKey(), pair.getValue());
                StorageDescriptor sd = table.getSd();
                List<FieldSchema> cols = sd.getCols();
                for (FieldSchema fieldSchema : cols)
                    columnNames.add(fieldSchema.getName());
                List<FieldSchema> partitionKeys = table.getPartitionKeys();
                for (FieldSchema fieldSchema : partitionKeys)
                    columnNames.add(fieldSchema.getName());
                return columnNames;
            } catch (TException e) {
                log.error("Hive MetaStore exception while finding " + tableName + " column names", e);

                if (i == maxRetries - 1) {
                    throw new TException(
                            "Failed to execute Hive MetaStore " + tableName + " column names due to : " + e.getMessage(), e);
                }
                retryGapInMillisecond = RetryWaitLogic.backOffAndWait(retryGapInMillisecond);
                hiveMetaStoreClient.reconnect();
            }
        }
        return columnNames;
    }

    public Set<String> getPartitionedColumnNames(String tableName) throws TException, TableNotFoundException {
        Pair<String, String> pair = getDbTableName(tableName);
        Set<String> partitionedColumnNames = new HashSet<>();
        int retryGapInMillisecond = retryGapInMillis;
        for (int i = 0; i < maxRetries; i++) {
            try {
                Table table = hiveMetaStoreClient.getTable(pair.getKey(), pair.getValue());
                List<FieldSchema> partitionKeys = table.getPartitionKeys();
                for (FieldSchema fieldSchema : partitionKeys)
                    partitionedColumnNames.add(fieldSchema.getName());
                return partitionedColumnNames;
            } catch (TException e) {
                log.error("Hive MetaStore exception while finding " + tableName + " partition column names", e);

                if (i == maxRetries - 1) {
                    throw new TException(
                            "Failed to execute Hive MetaStore " + tableName + " partition column names due to : " + e.getMessage(), e);
                }
                retryGapInMillisecond = RetryWaitLogic.backOffAndWait(retryGapInMillisecond);
                hiveMetaStoreClient.reconnect();
            }
        }
        return partitionedColumnNames;
    }

    public String getTableLocation(String tableName) throws TException, TableNotFoundException {
        Pair<String, String> pair = getDbTableName(tableName);
        int retryGapInMillisecond = retryGapInMillis;
        for (int i = 0; i < maxRetries; i++) {
            try {
                return hiveMetaStoreClient.getTable(pair.getKey(), pair.getValue()).getSd().getLocation();
            } catch (TException e) {
                log.error("Hive MetaStore exception while finding " + tableName + " location", e);

                if (i == maxRetries - 1) {
                    throw new TException(
                            "Failed to execute Hive MetaStore " + tableName + " location due to : " + e.getMessage(), e);
                }
                retryGapInMillisecond = RetryWaitLogic.backOffAndWait(retryGapInMillisecond);
                hiveMetaStoreClient.reconnect();
            }
        }
        return "";
    }

    public String getHiveTableStorageFormat(String tableName) throws TException, TableNotFoundException {
        Pair<String, String> pair = getDbTableName(tableName);
        int retryGapInMillisecond = retryGapInMillis;
        for (int i = 0; i < maxRetries; i++) {
            try {
                return "'" + hiveMetaStoreClient.getTable(pair.getKey(), pair.getValue()).getSd().getInputFormat() + "'";
            } catch (TException e) {
                log.error("Hive MetaStore exception while finding " + tableName + "Storage Format", e);

                if (i == maxRetries - 1) {
                    throw new TException(
                            "Failed to execute Hive MetaStore " + tableName + " Storage Format due to : " + e.getMessage(), e);
                }
                retryGapInMillisecond = RetryWaitLogic.backOffAndWait(retryGapInMillisecond);
                hiveMetaStoreClient.reconnect();
            }
        }
        return "";
    }

    public LinkedHashMap<String, String> getColumnsWithType(String tableName) throws TException, TableNotFoundException {
        Pair<String, String> pair = getDbTableName(tableName);
        LinkedHashMap<String, String> columnTypeMap = new LinkedHashMap<>();
        int retryGapInMillisecond = retryGapInMillis;
        for (int i = 0; i < maxRetries; i++) {
            try {
                try {
                    Table table = hiveMetaStoreClient.getTable(pair.getKey(), pair.getValue());
                    StorageDescriptor sd = table.getSd();
                    List<FieldSchema> cols = sd.getCols();
                    for (FieldSchema fieldSchema : cols)
                        columnTypeMap.put(fieldSchema.getName(), fieldSchema.getType());
                    List<FieldSchema> partitionKeys = table.getPartitionKeys();
                    for (FieldSchema fieldSchema : partitionKeys)
                        columnTypeMap.put(fieldSchema.getName(), fieldSchema.getType());
                    return columnTypeMap;
                } catch (NoSuchObjectException e) {
                    throw new TableNotFoundException(tableName, "Table not found", e);
                }
            } catch (TException e) {
                log.error("Hive MetaStore exception while finding " + tableName + "column schema", e);

                if (i == maxRetries - 1) {
                    throw new TException(
                            "Failed to execute Hive MetaStore " + tableName + "column schema due to : " + e.getMessage(), e);
                }
                retryGapInMillisecond = RetryWaitLogic.backOffAndWait(retryGapInMillisecond);
                hiveMetaStoreClient.reconnect();
            }
        }
        return columnTypeMap;
    }

    public String getHiveTableFieldDelimit(String tableName) throws TException, TableNotFoundException {
        String hiveTableFieldDelimi = "\\001";
        Pair<String, String> pair = getDbTableName(tableName);
        int retryGapInMillisecond = retryGapInMillis;
        for (int i = 0; i < maxRetries; i++) {
            try {
                Table table = hiveMetaStoreClient.getTable(pair.getKey(), pair.getValue());
                StorageDescriptor sd = table.getSd();
                SerDeInfo serdeInfo = sd.getSerdeInfo();
                Map<String, String> parameters = serdeInfo.getParameters();
                hiveTableFieldDelimi = parameters.get("field.delim");
                if (hiveTableFieldDelimi == null)
                    hiveTableFieldDelimi = parameters.get("separatorChar");
                if (hiveTableFieldDelimi == null)
                    return "\\001";
                return hiveTableFieldDelimi;
            } catch (TException e) {
                log.error("Hive MetaStore exception while finding " + tableName + " Field Delimit", e);

                if (i == maxRetries - 1) {
                    throw new TException(
                            "Failed to execute Hive MetaStore " + tableName + " Field Delimit due to : " + e.getMessage(), e);
                }
                retryGapInMillisecond = RetryWaitLogic.backOffAndWait(retryGapInMillisecond);
                hiveMetaStoreClient.reconnect();
            }
        }
        return hiveTableFieldDelimi;
    }

    public HiveTableDetails getHiveTableDetails(String tablename) throws TException, TableNotFoundException {
        Pair<String, String> pair = getDbTableName(tablename);
        String db_name = pair.getKey();
        String tableName = pair.getValue();
        int retryGapInMillisecond = retryGapInMillis;
        HiveTableDetails hiveTableDetails = new HiveTableDetails();
        for (int i = 0; i < maxRetries; i++) {
            try {
                hiveTableDetails.setTableName(tableName);
                hiveTableDetails.setDatabase(db_name);
                Table table = hiveMetaStoreClient.getTable(db_name, tableName);
                StorageDescriptor sd = table.getSd();
                hiveTableDetails.setLocation(sd.getLocation());

                List<TableColumn> columns = new LinkedList<>();
                List<TableColumn> partitionedColumns = new LinkedList<>();
                TableColumn tableColumn = new TableColumn();
                List<FieldSchema> cols = sd.getCols();
                for (FieldSchema fieldSchema : cols)
                    columns.add(new TableColumn(fieldSchema.getName(), fieldSchema.getType()));
                List<FieldSchema> partitionKeys = table.getPartitionKeys();
                for (FieldSchema fieldSchema : partitionKeys)
                    columns.add(new TableColumn(fieldSchema.getName(), fieldSchema.getType()));
                System.out.println(columns.toString());
                hiveTableDetails.setColumns(columns);

                Map<String, String> parameters = table.getParameters();
                StringBuilder tablePropertiesString = new StringBuilder();

                getProperties(tablePropertiesString, parameters);
                hiveTableDetails.setTableProperties(tablePropertiesString.toString());

                StringBuilder rowFormat = new StringBuilder();
                rowFormat.append("SERDE");
                rowFormat.append("'");
                rowFormat.append(sd.getSerdeInfo().getSerializationLib());
                rowFormat.append("'WITH SERDEPROPERTIES");
                rowFormat.append('(');
                Map<String, String> parameters1 = sd.getSerdeInfo().getParameters();
                getProperties(rowFormat, parameters1);
                rowFormat.append(')');
                hiveTableDetails.setRowFormat(rowFormat.toString());

                StringBuilder storedAsBloack = new StringBuilder();
                storedAsBloack.append("INPUTFORMAT");
                storedAsBloack.append(sd.getInputFormat());
                storedAsBloack.append("OUTPUTFORMAT");
                storedAsBloack.append(sd.getOutputFormat());
                hiveTableDetails.setStoredAsBlock(storedAsBloack.toString());

                String isEXTERNAL = parameters.get("EXTERNAL");
                if (isEXTERNAL == "TRUE")
                    hiveTableDetails.setExternal(true);

                for (FieldSchema fieldSchema : partitionKeys)
                    partitionedColumns.add(new TableColumn(fieldSchema.getName(), fieldSchema.getType()));
                hiveTableDetails.setPartitionedColumns(partitionedColumns);
                String dataType = "";
                hiveTableDetails.setDataType(dataType);
                return hiveTableDetails;
            } catch (TException e) {
                log.error("Hive MetaStore exception while finding " + tableName + " Table Details", e);

                if (i == maxRetries - 1) {
                    throw new TException(
                            "Failed to execute Hive MetaStore " + tableName + " Table Details due to : " + e.getMessage(), e);
                }
                retryGapInMillisecond = RetryWaitLogic.backOffAndWait(retryGapInMillisecond);
                hiveMetaStoreClient.reconnect();
            }
        }
        return hiveTableDetails;
    }

    public boolean checkPartitionExists(String tableName, String partitionCol, String refreshId) throws TableNotFoundException, TException {
        Pair<String, String> pair = getDbTableName(tableName);
        int retryGapInMillisecond = retryGapInMillis;
        if (!getPartitionedColumnNames(tableName).contains(partitionCol))
            return false;
        for (int i = 0; i < maxRetries; i++) {
            try {
                List<String> listPartitionNames = hiveMetaStoreClient.listPartitionNames(pair.getKey(), pair.getValue(), (short) -1);
                for (String partitionNames : listPartitionNames)
                    if (partitionNames.contains(partitionCol + "=" + refreshId))
                        return true;
                return false;
            } catch (TException e) {
                log.error("Hive MetaStore exception while validating whether partition " + partitionCol + "=" + refreshId + " exists or not in Table : " + tableName, e);

                if (i == maxRetries - 1) {
                    throw new TException(
                            "Failed to execute Hive MetaStore validation whether partition " + partitionCol + "=" + refreshId + " exists or not in Table : " + tableName + " due to : " + e.getMessage(), e);
                }
                retryGapInMillisecond = RetryWaitLogic.backOffAndWait(retryGapInMillisecond);
                hiveMetaStoreClient.reconnect();
            }
        }
        return false;
    }

    private void getProperties(StringBuilder tablePropertiesString, Map<String, String> parameters) {
        boolean moreThanOneProperties = false;
        for (Map.Entry<String, String> para : parameters.entrySet()) {
            if (moreThanOneProperties)
                tablePropertiesString.append(",");
            tablePropertiesString.append("'");
            tablePropertiesString.append(para.getKey());
            tablePropertiesString.append("'");
            tablePropertiesString.append("=");
            tablePropertiesString.append("'");
            tablePropertiesString.append(para.getValue());
            tablePropertiesString.append("'");
            moreThanOneProperties = true;
        }
    }

    private Pair<String, String> getDbTableName(String tableName) throws TableNotFoundException {
        int dotIndex = tableName.indexOf('.');
        if (dotIndex == -1)
            throw new TableNotFoundException(tableName, "'.' is missing in table name");
        String db_name = tableName.substring(0, dotIndex);
        String tablename = tableName.substring(dotIndex + 1);
        return new Pair<>(db_name, tablename);
    }

    public List<String> getTablesByPattern(String db, String pattern) throws TException {
        int retryGapInMillisecond = retryGapInMillis;
        for (int i = 0; i < maxRetries; i++) {
            try {
                return hiveMetaStoreClient.getTables(db, pattern);
            } catch (TException e) {
                log.error("Hive MetaStore exception while finding tables with pattern: " + pattern  + " for db: " + db, e);

                if (i == maxRetries - 1) {
                    throw new TException(
                            "Failed to execute Hive MetaStore Query for finding table with pattern: " + pattern + " for db: " + db + e.getMessage(), e);
                }
                retryGapInMillisecond = RetryWaitLogic.backOffAndWait(retryGapInMillisecond);
                hiveMetaStoreClient.reconnect();
            }
        }
        return new ArrayList<>();
    }
}
