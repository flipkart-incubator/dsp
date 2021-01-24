package com.flipkart.dsp.qe.clients;

import com.flipkart.dsp.qe.entity.HiveConfigParam;
import com.flipkart.dsp.qe.entity.HiveTableDetails;
import com.flipkart.dsp.qe.exceptions.HiveClientException;
import com.flipkart.dsp.qe.exceptions.TableNotFoundException;
import com.flipkart.dsp.qe.utils.RetryWaitLogic;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 */

@Slf4j
@Singleton
public class HiveClient {

    private static final String QUERY_SHOW_COL = "SHOW COLUMNS IN %s";
    private static final String SHOW_CREATE_TABLE = "SHOW CREATE TABLE %s";
    private static final String DESC_FORMATTED_TABLE = "DESCRIBE FORMATTED %s";
    private static final String SELECT_ALL = "SELECT * FROM %s limit 1";
    private static final String MAX_REFRESH = "SELECT max(refresh_id) from %s";
    private static final String GENERIC_QUERY_EXEC_ERROR_MSG = "Query could not be executed %s";
    private static final String PARTITION_EXISTS = "SHOW PARTITIONS %s PARTITION(`%s`=%s)";
    private static final String PARTITION_DATA_EXISTS = "SELECT * FROM %s WHERE `%s`=%s limit 1";
    private static final String MAPRED_QUEUE_NAME_PROPERTY = "mapred.job.queue.name";
    private static final String HIVE_QUEUE_CONFIG = "?" + MAPRED_QUEUE_NAME_PROPERTY + "=%s";
    private final HiveConfigParam hiveConfigParam;

    public void setQueue(String queueName) {
        log.info("Setting HIVE queue to : {}", queueName);
        String url = hiveConfigParam.getUrl();
        if (!url.contains(MAPRED_QUEUE_NAME_PROPERTY))
            hiveConfigParam.setUrl(url + String.format(HIVE_QUEUE_CONFIG, queueName));
    }

    @Inject
    public HiveClient(HiveConfigParam hiveConfigParam) {
        this.hiveConfigParam = hiveConfigParam;
    }

    public void executeQuery(Set<String> queries) throws HiveClientException {
        for (String query : queries) {
            executeQuery(query);
        }
    }

    public Connection getConnection() throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Class.forName("com.google.common.util.concurrent.MoreExecutors");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(hiveConfigParam.getUrl(), hiveConfigParam.getUser(), hiveConfigParam.getPassword());
    }

    public void executeQuery(String query) throws HiveClientException {
        int retryGapInMillis = hiveConfigParam.getRetryGapInMillis();
        for (int i = 0; i < hiveConfigParam.getMaxRetries(); i++) {
            try (Connection conn = getConnection(); Statement smt = conn.createStatement()) {
                smt.execute(query);
                return;
            } catch (SQLException e) {
                log.error("SQL exception while executing Hive Query. Query : " + query, e);

                if (i == hiveConfigParam.getMaxRetries() - 1) {
                    throw new HiveClientException(
                            "Failed to execute query : \"" + query + "\" due to : " + e.getMessage(), e);
                }

                retryGapInMillis = RetryWaitLogic.backOffAndWait(retryGapInMillis);
            }
        }
    }

    public List<Map<String, Object>> executeAndFetch(String query) throws HiveClientException {
        int retryGapInMillis = hiveConfigParam.getRetryGapInMillis();

        for (int i = 0; i < hiveConfigParam.getMaxRetries(); i++) {
            try (Connection conn = getConnection(); Statement smt = conn
                    .createStatement(); ResultSet rs = smt.executeQuery(query)) {

                ResultSetMetaData metaData = rs.getMetaData();
                List<String> colNameList = getColumnName(metaData);

                return getRowsFromResultSet(rs, colNameList);
            } catch (SQLException e) {
                log.error("SQL exception while executing Hive Query. Query : " + query, e);

                if (i == hiveConfigParam.getMaxRetries() - 1) {
                    throw new HiveClientException(
                            "Failed to execute query : \"" + query + "\" due to : " + e.getMessage(), e);
                }
                retryGapInMillis = RetryWaitLogic.backOffAndWait(retryGapInMillis);
            }
        }

        throw new HiveClientException(format(GENERIC_QUERY_EXEC_ERROR_MSG, query));
    }

    private List<Map<String, Object>> getRowsFromResultSet(ResultSet rs, List<String> colNameList) throws SQLException {
        List<Map<String, Object>> rowList = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (String colName : colNameList) {
                Object val = rs.getObject(colName);
                row.put(colName, val);
            }
            rowList.add(row);
        }
        return rowList;
    }

    private ArrayList<String> getColumnName(ResultSetMetaData metaData) throws SQLException {
        int colCount = metaData.getColumnCount();
        ArrayList<String> colList = new ArrayList<>();
        for (int index = 1; index <= colCount; index++) {
            colList.add(metaData.getColumnName(index));
        }
        return colList;
    }

    public String getTableLocation(String tableName) throws HiveClientException {
        int retryGapInMillis = hiveConfigParam.getRetryGapInMillis();
        String query = format(SHOW_CREATE_TABLE, tableName);

        for (int i = 0; i < hiveConfigParam.getMaxRetries(); i++) {
            try (Connection conn = getConnection(); Statement smt = conn
                    .createStatement(); ResultSet rs = smt.executeQuery(query)) {
                while (rs.next()) {
                    if (rs.getString(1).equalsIgnoreCase("location")) {
                        rs.next();
                        break;
                    }
                }
                return rs.getString(1).replaceAll("\'", "").trim();
            } catch (SQLException e) {
                log.error("SQL exception while executing Hive Query. Query : " + query, e);

                if (i == hiveConfigParam.getMaxRetries() - 1) {
                    throw new HiveClientException(
                            "Failed to execute query : \"" + query + "\" due to : " + e.getMessage(), e);
                }
                retryGapInMillis = RetryWaitLogic.backOffAndWait(retryGapInMillis);
            }

        }
        throw new HiveClientException("Error in executing query: " + query);
    }

    public ArrayList<String> getColumnNames(String tableName) throws HiveClientException {
        int retryGapInMillis = hiveConfigParam.getRetryGapInMillis();
        String query = format(QUERY_SHOW_COL, tableName);

        for (int i = 0; i < hiveConfigParam.getMaxRetries(); i++) {
            try (Connection conn = getConnection(); Statement smt = conn
                    .createStatement(); ResultSet rs = smt.executeQuery(query)) {

                ResultSetMetaData metaData = rs.getMetaData();
                ArrayList<String> columnNames = new ArrayList<>();
                while (rs.next()) {
                    columnNames.add(rs.getString(metaData.getColumnName(1)));
                }

                return columnNames;
            } catch (SQLException e) {
                log.error("SQL exception while executing Hive Query. Query : " + query, e);

                if (i == hiveConfigParam.getMaxRetries() - 1) {
                    throw new HiveClientException(
                            "Failed to execute query : \"" + query + "\" due to : " + e.getMessage(), e);
                }
                retryGapInMillis = RetryWaitLogic.backOffAndWait(retryGapInMillis);
            }

        }

        throw new HiveClientException(format(GENERIC_QUERY_EXEC_ERROR_MSG, query));
    }

    public void updatePartitions(String tableName, Set<LinkedHashMap<String, String>> partitionMapSet,
                                 String operation) throws HiveClientException {

        StringBuilder stringBuilder = new StringBuilder();
        boolean outerFirst = true;
        int count = 0;
        int totalCount = 0;
        for (Map<String, String> partitionMap : partitionMapSet) {
            if (outerFirst) {
                outerFirst = false;
            } else {
                if (operation.contains("ADD")) {
                    stringBuilder.append(" ");
                } else {
                    stringBuilder.append(",");
                }
            }
            boolean innerFirst = true;
            for (Map.Entry<String, String> entry : partitionMap.entrySet()) {
                if (innerFirst) {
                    stringBuilder.append("PARTITION ( ").append(entry.getKey()).append("=").append("'").append(entry.getValue()).append("'");
                    innerFirst = false;
                } else {
                    stringBuilder.append(",").append(entry.getKey()).append("=").append("'").append(entry.getValue()).append("'");
                }
            }
            stringBuilder.append(" )");
            if (count++ > 250) {
                String query = "ALTER TABLE " + tableName + " " + operation + " " + stringBuilder.toString();
                executeQuery(query);
                stringBuilder = new StringBuilder();
                outerFirst = true;
                count = 0;
            }
            totalCount++;
        }
        String query = "ALTER TABLE " + tableName + " " + operation + " " + stringBuilder.toString();
        if (!stringBuilder.toString().isEmpty()) {
            log.info("Executing query: {}", query);
            executeQuery(query);
        }
        log.info("Persisted {} partitions", totalCount);
    }

    public Set<String> getPartitionedColumnNames(String tableName) throws HiveClientException {
        int retryGapInMillis = hiveConfigParam.getRetryGapInMillis();
        String query = format(SHOW_CREATE_TABLE, tableName);
        LinkedHashSet<String> partitions = new LinkedHashSet<>();

        for (int i = 0; i < hiveConfigParam.getMaxRetries(); i++) {
            try (Connection conn = getConnection(); Statement smt = conn.createStatement(); ResultSet rs = smt.executeQuery(query)) {
                while (rs.next()) {
                    if (rs.getString(1).equalsIgnoreCase("PARTITIONED BY ( ")) {
                        while (rs.next()) {
                            partitions.add(rs.getString(1));
                            if (rs.getString(1).contains(")")) {
                                break;
                            }
                        }
                        break;
                    }
                }
            } catch (SQLException e) {
                log.error("SQL exception while executing Hive Query. Query : " + query, e);

                if (i == hiveConfigParam.getMaxRetries() - 1) {
                    throw new HiveClientException(
                            "Failed to execute query : \"" + query + "\" due to : " + e.getMessage(), e);
                }
                retryGapInMillis = RetryWaitLogic.backOffAndWait(retryGapInMillis);
            }
        }

        return partitions.stream().
                map(p -> p.trim().split(" ")[0].replaceAll("`", "").trim())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public String getHiveTableStorageFormat(String tableName) {
        String query = format(SHOW_CREATE_TABLE, tableName);

        try (Connection conn = getConnection(); Statement smt = conn.createStatement(); ResultSet rs = smt.executeQuery(query)) {
                while (rs.next()) {
                    if (rs.getString(1).toUpperCase().contains("STORED AS INPUTFORMAT")) {
                        rs.next();
                        return rs.getString(1).trim();
                    }
                }
            } catch (SQLException e) {
                log.error("SQL exception while executing Hive Query to retrieve input/output format. Query : " + query, e);
            }
        return null;
    }

    public LinkedHashMap<String, String> getColumnsWithType(String tableName) throws HiveClientException, TableNotFoundException {
        int retryGapInMillis = hiveConfigParam.getRetryGapInMillis();
        String query = format(SELECT_ALL, tableName);

        for (int i = 0; i < hiveConfigParam.getMaxRetries(); i++) {
            try (Connection conn = getConnection(); Statement smt = conn
                    .createStatement(); ResultSet rs = smt.executeQuery(query)) {

                ResultSetMetaData metaData = rs.getMetaData();
                LinkedHashMap<String, String> columnTypeMap = new LinkedHashMap<>();
                for (int idx=1; idx <= metaData.getColumnCount(); idx++) {
                    String columnName = metaData.getColumnName(idx).split("\\.")[1];
                    columnTypeMap.put(columnName, metaData.getColumnTypeName(idx));
                }
                return columnTypeMap;
            } catch (SQLException e) {
                if (e.getMessage().contains("Table not found")){
                    throw new TableNotFoundException(tableName, e.getMessage());
                }
                log.error("SQL exception while executing Hive Query. Query : " + query, e);
                if (i == hiveConfigParam.getMaxRetries() - 1) {
                    throw new HiveClientException(
                            "Failed to execute query : \"" + query + "\" due to : " + e.getMessage(), e);
                }
                retryGapInMillis = RetryWaitLogic.backOffAndWait(retryGapInMillis);
            }

        }

        throw new HiveClientException(format(GENERIC_QUERY_EXEC_ERROR_MSG, query));
    }

    public HiveTableDetails getHiveTableDetails(String tableName) throws HiveClientException {
        String query = format(SHOW_CREATE_TABLE, tableName);
        StringBuilder createTable = new StringBuilder();

        try (Connection conn = getConnection(); Statement smt = conn.createStatement(); ResultSet rs = smt.executeQuery(query)) {
            log.info("Hive ConnectionID {} for Thread {} ", conn, Thread.currentThread());
            while (rs.next()) {
                createTable.append(" ").append(rs.getString(1));
            }
        } catch (SQLException e) {
            log.error("SQL exception while executing Hive Query to retrieve Create Table Details. Query : " + query, e);
            throw new HiveClientException("Error In Executing Query " + e);
        }
        return new HiveTableDetails(createTable.toString());
    }

    public Long getLatestRefreshId(String tableName) throws HiveClientException {
        int retryGapInMillis = hiveConfigParam.getRetryGapInMillis();
        String query = format(MAX_REFRESH,tableName);
        for (int i = 0; i < hiveConfigParam.getMaxRetries(); i++) {
            try (Connection conn = getConnection(); Statement smt = conn.createStatement(); ResultSet rs = smt.executeQuery(query)) {
                log.info("Hive ConnectionID {} for Thread {} ", conn, Thread.currentThread());
                rs.next();
                return Long.valueOf(rs.getString(1));
            } catch (SQLException e) {
                log.error("SQL exception while executing Hive Query to retrieve latest refresh_id. Query : " + query, e);
                if (i == hiveConfigParam.getMaxRetries() - 1) {
                    throw new HiveClientException(
                            "Failed to execute query : \"" + query + "\" due to : " + e.getMessage(), e);
                }
                retryGapInMillis = RetryWaitLogic.backOffAndWait(retryGapInMillis);
            }
        }
        throw new HiveClientException(format(GENERIC_QUERY_EXEC_ERROR_MSG, query));
    }

    public String getHiveTableFieldDelimit(String tableName) throws HiveClientException {
        int retryGapInMillis = hiveConfigParam.getRetryGapInMillis();
        String fieldDelimiter = "\\001";
        String query = format(DESC_FORMATTED_TABLE, tableName);

        for (int i = 0; i < hiveConfigParam.getMaxRetries(); i++) {
            try (Connection conn = getConnection(); Statement smt = conn.createStatement(); ResultSet rs = smt.executeQuery(query)) {
                log.info("Hive ConnectionID {} for Thread {} ", conn, Thread.currentThread());
                while (rs.next()) {
                    if (rs.getString(2)!=null &&
                            (rs.getString(2).contains("field.delim") || rs.getString(2).contains("separatorChar"))) {
                        return rs.getString(3).trim();
                    }
                }
                return fieldDelimiter;
            } catch (SQLException e) {
                log.error("SQL exception while executing Hive Query to retrieve input/output format. Query : " + query, e);
                if (i == hiveConfigParam.getMaxRetries() - 1) {
                    throw new HiveClientException(
                            "Failed to execute query : \"" + query + "\" due to : " + e.getMessage(), e);
                }
                retryGapInMillis = RetryWaitLogic.backOffAndWait(retryGapInMillis);
            }
        }
        return fieldDelimiter;
    }

    public Boolean checkPartitionExists(String tableName, String partitionCol, String refreshId) throws HiveClientException {
        int retryGapInMillis = hiveConfigParam.getRetryGapInMillis();
        String query = format(PARTITION_EXISTS, tableName, partitionCol, refreshId);

        for (int i = 0; i < hiveConfigParam.getMaxRetries(); i++) {
            try (Connection conn = getConnection(); Statement smt = conn.createStatement(); ResultSet rs = smt.executeQuery(query)) {
                log.info("Hive ConnectionID {} for Thread {} ", conn, Thread.currentThread());
                while (rs.next()) {
                    if (rs.getString(1).contains(refreshId)) {
                        return true;
                    }
                }
                return false;
            } catch (SQLException e) {
                log.error("SQL exception while executing Hive Query to retrieve input/output format. Query : " + query, e);
                if (i == hiveConfigParam.getMaxRetries() - 1) {
                    throw new HiveClientException(
                            "Failed to execute query : \"" + query + "\" due to : " + e.getMessage(), e);
                }
                retryGapInMillis = RetryWaitLogic.backOffAndWait(retryGapInMillis);
            }
        }
        return false;
    }

    public Boolean checkPartitionHasData(String tableName, String partitionCol, String refreshId) throws HiveClientException {
        int retryGapInMillis = hiveConfigParam.getRetryGapInMillis();
        String query = format(PARTITION_DATA_EXISTS, tableName, partitionCol, refreshId);

        for (int i = 0; i < hiveConfigParam.getMaxRetries(); i++) {
            try (Connection conn = getConnection(); Statement smt = conn.createStatement(); ResultSet rs = smt.executeQuery(query)) {
                log.info("Hive ConnectionID {} for Thread {} ", conn, Thread.currentThread());
                if (rs.next()) {
                   return true;
                }
                return false;
            } catch (SQLException e) {
                log.error("SQL exception while executing Hive Query to retrieve input/output format. Query : " + query, e);
                if (i == hiveConfigParam.getMaxRetries() - 1) {
                    throw new HiveClientException(
                            "Failed to execute query : \"" + query + "\" due to : " + e.getMessage(), e);
                }
                retryGapInMillis = RetryWaitLogic.backOffAndWait(retryGapInMillis);
            }
        }
        return false;
    }

}

