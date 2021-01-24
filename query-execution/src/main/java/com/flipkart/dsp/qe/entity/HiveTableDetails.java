package com.flipkart.dsp.qe.entity;

import com.flipkart.dsp.qe.exceptions.HiveClientException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Getter
@Setter
@Slf4j
public class HiveTableDetails {
    private String dataType = "";
    private String database = "";
    private String rowFormat = "";
    private String location = "";
    private String tableName = "";
    private String dataTypeName = "";
    private String storedAsBlock = "";
    private String storedByBlock = "";
    private boolean isExternal = false;
    private String tableProperties = "";
    private static final String TABLE_TOKEN = "TABLE ";
    private static final String EXTERNAL_TOKEN = "EXTERNAL ";
    private List<TableColumn> columns =  new LinkedList<>();
    private List<TableColumn> partitionedColumns = new LinkedList<>();
    private static final String TABLE_FAIL_SAFE_TOKEN = "TABLE IF NOT EXISTS ";
    private static final Set<String> NON_PRIMITIVE_TYPES =
            new HashSet<>(Arrays.asList("array", "map", "struct", "uniontype"));

    public HiveTableDetails() {}

    public HiveTableDetails(String createCommand) throws HiveClientException {
        deserialize(createCommand);
    }

    public String serialize() {
        StringBuilder createCommand = new StringBuilder("CREATE ");
        createCommand.append(isExternal ? EXTERNAL_TOKEN : "");
        createCommand.append(TABLE_FAIL_SAFE_TOKEN);
        createCommand.append(database.length() > 0 && !tableName.contains(".") ? database + "." : "");
        createCommand.append(tableName).append(" ");
        createCommand.append("( ").append(serializeColumns(columns)).append(" ) ");

        createCommand.append(validateAndAddAttribute(" PARTITIONED BY ( ", partitionedColumns.size(), serializeColumns(partitionedColumns)));
        if (partitionedColumns.size() != 0) createCommand.append( " ) ");
        createCommand.append(validateAndAddAttribute(" ROW FORMAT ", rowFormat.length(), rowFormat));
        createCommand.append(validateAndAddAttribute(" STORED AS ", storedAsBlock.length(), storedAsBlock));
        createCommand.append(validateAndAddAttribute(" STORED BY ", storedByBlock.length(), storedByBlock));
        createCommand.append(validateAndAddAttribute(" LOCATION ", location.length(), location));
        createCommand.append(validateAndAddAttribute(" TBLPROPERTIES ", tableProperties.length(), tableProperties));

        String createCommandString  = createCommand.toString();
        return createCommandString.replaceAll(" {2}", " ");
    }

    private String validateAndAddAttribute(String prefix, int objectSize, String object) {
        StringBuilder string = new StringBuilder();
        if (!(Objects.isNull(object)) && objectSize > 0)
            string.append(prefix).append(object);
        return string.toString();
    }

    private String serializeColumns(List<HiveTableDetails.TableColumn> columns) {
        StringBuilder columnsString = new StringBuilder();
        for (TableColumn column : columns) {
            columnsString.append(columnsString.length() > 0 ? ", " : "");
            columnsString.append(standardiseColumnName(column.getColumnName())).append(" ");
            columnsString.append(column.getColumnType());
        }
        return columnsString.toString();
    }

    private String standardiseColumnName(String origColumnName) {
        if (!origColumnName.startsWith("`")) origColumnName = "`" + origColumnName;
        if (!origColumnName.endsWith("`")) origColumnName = origColumnName + "`";
        return origColumnName;
    }

    private void deserialize(String createCommand) throws HiveClientException {
        if (createCommand.endsWith(";"))
            createCommand = createCommand.substring(0, createCommand.length() - 1);
        createCommand = formatInput(createCommand);

        extractTableType(createCommand);
        extractTableName(createCommand);
        this.columns = getEntityColumns(createCommand);
        this.partitionedColumns = extractPartitionColumns(createCommand);
        String commandExcludingColumns = getCommandWithoutColumns(createCommand);
        extractLocationBlock(commandExcludingColumns);
        extractStoredBlock(commandExcludingColumns);
        extractRowFormat(commandExcludingColumns);
        extractTableProperties(commandExcludingColumns);
    }

    private String formatInput(String createCommand) {
        createCommand = createCommand.replace("\n", " ").replace("\r", " ").trim();
        while (createCommand.contains("  ")) {
            createCommand = createCommand.replace("  ", " ");
        }
        return createCommand;
    }

    private void extractTableType(String createCommand) {
        this.isExternal = createCommand.toLowerCase().contains("external table");
    }

    private void extractTableName(String createCommand) {
        int tableNameStartIndex;
        int tableIndex = createCommand.toLowerCase().indexOf(TABLE_FAIL_SAFE_TOKEN.toLowerCase());
        if (tableIndex == -1) {
            tableIndex = createCommand.toLowerCase().indexOf(TABLE_TOKEN.toLowerCase());
            tableNameStartIndex = tableIndex + TABLE_TOKEN.length();
        } else {
            tableNameStartIndex = tableIndex + TABLE_FAIL_SAFE_TOKEN.length();
        }

        int tableNameEndIndex = createCommand.indexOf("(", tableNameStartIndex);
        String tableName = createCommand.substring(tableNameStartIndex, tableNameEndIndex).trim();
        if (tableName.contains(".")) {
            this.database = tableName.substring(0, tableName.indexOf(".")).replace("`", "");
            this.tableName = tableName.substring(tableName.indexOf(".") + 1).replace("`", "");
            return;
        }
        this.tableName = tableName;
    }

    private List<TableColumn> getEntityColumns(String createCommand) {
        int startIndex = createCommand.indexOf("(");
        int endIndex = createCommand.indexOf(")", startIndex);
        return getColumns(startIndex, endIndex, createCommand);
    }

    private List<TableColumn> getColumns(int startIndex, int endIndex, String createCommand) {

        List<TableColumn> columns = new ArrayList<>();
        createCommand = createCommand.substring(startIndex + 1, endIndex).trim();

        for (int i = 0; i < createCommand.length(); i++) {
            if (createCommand.charAt(i) == ' ' || createCommand.charAt(i) == '\t') {
                continue;
            }

            String columnName = createCommand.substring(i, createCommand.indexOf(' ', i));
            i += columnName.length();
            i = getColumnDataType(i, createCommand);
            columns.add(TableColumn.builder().columnName(standardiseColumnName(columnName))
                    .columnType(dataType).build());
        }
        return columns;
    }

    private int getColumnDataType(int columnNameEndIndex, String createCommand) {
        int dataTypeNameEndIndex = extractDataTypeName(columnNameEndIndex, createCommand);
        return extractDataTypeDetails(dataTypeNameEndIndex, createCommand);
    }

    private int extractDataTypeName(int index, String createCommand) {
        while (index < createCommand.length() && createCommand.charAt(index) == ' ') {
            index++;
        }

        int angleBracketPosition = createCommand.indexOf('<', index) == -1 ?
                Integer.MAX_VALUE : createCommand.indexOf('<', index);
        int commaPosition = createCommand.indexOf(',', index) == -1 ?
                Integer.MAX_VALUE : createCommand.indexOf(',', index);
        int spacePosition = createCommand.indexOf(' ', index) == -1 ?
                Integer.MAX_VALUE : createCommand.indexOf(' ', index);
        int stringLength = createCommand.length();
        int tokenEndIndex = Math.min(Math.min(
                Math.min(angleBracketPosition, commaPosition), spacePosition), stringLength);

        this.dataTypeName = createCommand.substring(index, tokenEndIndex);
        return tokenEndIndex;
    }

    private int extractDataTypeDetails(int dataTypeNameEndIndex, String createCommand) {

        boolean isPrimitive = isPrimitiveDataType(dataTypeNameEndIndex, createCommand);

        if (!isPrimitive) {
            return extractNonPrimitiveDataTypeDetails(dataTypeNameEndIndex, createCommand);
        }
        return extractPrimitiveDataTypeDetails(dataTypeNameEndIndex, createCommand);
    }

    private int extractPrimitiveDataTypeDetails(int dataTypeNameEndIndex, String createCommand) {
        this.dataType = this.dataTypeName;
        int commaPosition = createCommand.indexOf(",", dataTypeNameEndIndex);

        if (commaPosition == -1)
            return createCommand.length();
        return commaPosition;
    }

    private int extractNonPrimitiveDataTypeDetails(int dataTypeNameEndIndex, String createCommand) {
        Stack<Character> operatorStack = new Stack<>();
        int j = dataTypeNameEndIndex;
        while (j < createCommand.length() && createCommand.charAt(j) != '<') {
            j++;
        }

        operatorStack.push(createCommand.charAt(j++));
        while (!operatorStack.isEmpty()) {
            if (createCommand.charAt(j) != '<' &&
                    createCommand.charAt(j) != '>') {
                j++;
            } else if (createCommand.charAt(j) == '<') {
                operatorStack.push('<');
                j++;
            } else if (createCommand.charAt(j) == '>') {
                operatorStack.pop();
                j++;
            }
        }

        this.dataType = this.dataTypeName + createCommand.substring(dataTypeNameEndIndex, j);
        return createCommand.indexOf(',', j) == -1 ? createCommand.length() - 1 :
                createCommand.indexOf(',', j);
    }


    private boolean isPrimitiveDataType(int dataTypeNameEndIndex, String createCommand) {
        int angleBracketPosition = createCommand.indexOf('<', dataTypeNameEndIndex) == -1 ?
                Integer.MAX_VALUE : createCommand.indexOf('<', dataTypeNameEndIndex);

        return (dataTypeNameEndIndex != angleBracketPosition) &&
                (!NON_PRIMITIVE_TYPES.contains(this.dataTypeName.toLowerCase()));
    }


    private List<TableColumn> extractPartitionColumns(String createCommand) {
        int partitionIndex = createCommand.toLowerCase().indexOf("partitioned by");
        if (partitionIndex > 0) {
            int startIndex = createCommand.indexOf("(", partitionIndex);
            int endIndex = createCommand.indexOf(")", startIndex);
            return getColumns(startIndex, endIndex, createCommand);
        }
        return new LinkedList<>();
    }

    private String getCommandWithoutColumns(String createCommand) {
        int colStartIndex = createCommand.indexOf("(");
        int colEndIndex = createCommand.indexOf(")", colStartIndex);

        String columns = createCommand.substring(colStartIndex, colEndIndex + 1);
        String commandWithoutColumns = createCommand.replace(columns, "");

        int partitionIndex = createCommand.toLowerCase().indexOf("partitioned by");
        if (partitionIndex > 0) {
            int partStartIndex = createCommand.indexOf("(", partitionIndex);
            int partEndIndex = createCommand.indexOf(")", partStartIndex);
            String partition = createCommand.substring(partitionIndex, partEndIndex + 1);
            commandWithoutColumns = commandWithoutColumns.replace(partition, "");
        }

        return commandWithoutColumns;
    }

    private void extractLocationBlock(String createCommand) {
        if (createCommand.toLowerCase().contains("location")) {
            int locationIndex = createCommand.toLowerCase().indexOf("location");
            int locationEndIndex = createCommand.indexOf(" ", locationIndex + 9);
            if (locationEndIndex == -1) locationEndIndex = createCommand.length();
            location = createCommand.substring(locationIndex + 9, locationEndIndex);
        }
    }

    private void extractStoredBlock(String command) throws HiveClientException {
        if (!extractStoredAsBlock(command))
            extractStoredByBlock(command);
    }

    private boolean extractStoredAsBlock(String command) throws HiveClientException {
        if (command.toLowerCase().contains("stored as")) {
            if(!extractStoredAsFileDetails(command))
                return extractStoredAsFormatDetails(command);
            return true;
        }
        return false;
    }

    private boolean extractStoredAsFileDetails(String command) {
        int storedAsIndex = command.toLowerCase().indexOf("stored as");
        Set<String> fileTypes =
                new HashSet<>(Arrays.asList("sequencefile", "textfile", "rcfile", "orc", "parquet","avro"));

        for (String fileType : fileTypes) {
            if (command.toLowerCase().contains(fileType)) {
                int storedAsEndIndex = command.indexOf(" ", storedAsIndex + 10);
                if (storedAsEndIndex == -1) storedAsEndIndex = command.length();
                storedAsBlock = command.substring(storedAsIndex + 10, storedAsEndIndex);
                return true;
            }
        }
        return false;
    }

    private boolean extractStoredAsFormatDetails(String command) throws HiveClientException {
        int storedAsIndex = command.toLowerCase().indexOf("stored as");
        if (command.toLowerCase().contains("inputformat ")) {
            int outputFormatBlockEndIndex = command.indexOf(" ",
                    command.toLowerCase().indexOf("outputformat") + 13);
            int storedAsEndIndex = outputFormatBlockEndIndex == -1 ? -1 : command.indexOf(" ", outputFormatBlockEndIndex);
            if (storedAsEndIndex == -1) {
                storedAsEndIndex = command.length();
            }
            storedAsBlock = command.substring(storedAsIndex + 10, storedAsEndIndex);
            return true;
        }
        throw new HiveClientException("Invalid or Incomplete stored as details for hive table while deserialization");
    }


    private void extractStoredByBlock(String command) throws HiveClientException {
        if (command.toLowerCase().contains("stored by"))
            extractSerDeDetails(command);
    }

    private void extractSerDeDetails(String command) throws HiveClientException {
        int storedByIndex = command.toLowerCase().indexOf("stored by");
        int serDePropertiesIndex = command.toLowerCase().indexOf("with serdeproperties");
        if (serDePropertiesIndex != -1) {
            int earliestEndMarker = command.indexOf(" ", serDePropertiesIndex + 21);
            if (earliestEndMarker != -1) {
                earliestEndMarker = command.length();
            }
            storedByBlock = command.substring(storedByIndex + 10, earliestEndMarker);
        } else
            throw new HiveClientException("Invalid or Incomplete stored by details for hive table while deserialization");
    }

    private void extractRowFormat(String command) {
        if (command.toLowerCase().contains("row format")) {
            if (!extractRowSerDeInformation(command))
                extractRoWFormatInformation(command);
        }
    }

    private boolean extractRowSerDeInformation(String command) {
        int rowFormatIndex = command.toLowerCase().indexOf("row format");
        if (command.toLowerCase().indexOf("serde ", rowFormatIndex) == command.toLowerCase().indexOf("row format") + 11) {
            int serDeStartIndex = command.toLowerCase().indexOf("serde ", rowFormatIndex);
            int serDeEndIndex;
            if (command.toLowerCase().indexOf("serdeproperties", serDeStartIndex) != -1)
                serDeEndIndex = command.indexOf(")", command.toLowerCase().indexOf("serdeproperties", serDeStartIndex));
            else {
                serDeEndIndex = command.indexOf(" ", serDeStartIndex + 6);
                if (serDeEndIndex == -1) serDeEndIndex = command.length();
            }

            rowFormat = command.substring(serDeStartIndex, serDeEndIndex);
            return true;
        }
        return false;
    }

    private void extractRoWFormatInformation(String command) {
        int rowFormatIndex = command.toLowerCase().indexOf("row format");
        int delimitedStartIndex = command.toLowerCase().indexOf("delimited ", rowFormatIndex);

        int escapedIndex = getIndex(command, "escaped by ", delimitedStartIndex);
        int fieldsIndex = getIndex(command, "fields terminated by ", delimitedStartIndex);
        int nullDefinedIndex = getIndex(command, "null defined as ", delimitedStartIndex);
        int mapKeysIndex = getIndex(command, "map keys terminated by ", delimitedStartIndex);
        int linesTerminationIndex = getIndex(command, "lines terminated by ", delimitedStartIndex);
        int collectionIndex = getIndex(command, "collection items terminated by ", delimitedStartIndex);

        int relevantIndex = Math.max(fieldsIndex, Math.max(escapedIndex,
                Math.max(collectionIndex, Math.max(mapKeysIndex, Math.max(linesTerminationIndex, nullDefinedIndex)))));

        int relevantEndIndex = command.indexOf(" ", relevantIndex);
        if (relevantEndIndex == -1) relevantEndIndex = command.length();
        rowFormat = command.substring(delimitedStartIndex, relevantEndIndex);
    }

    private int getIndex(String input,String indexOf, int from) {
        int index = input.toLowerCase().indexOf(indexOf, from);
        return (index == -1 ? index : index + indexOf.length());
    }

    private void extractTableProperties(String createCommand) throws HiveClientException {
        if (createCommand.toLowerCase().contains("tblproperties")) {
            int tblPropertiesIndex = createCommand.toLowerCase().indexOf("tblproperties");
            int tblPropertiesBlockStartIndex = createCommand.indexOf("(", tblPropertiesIndex);
            int tblPropertiesBlockEndIndex = createCommand.indexOf(")", tblPropertiesBlockStartIndex);
            if (tblPropertiesBlockEndIndex == -1 || tblPropertiesBlockEndIndex + 1 != createCommand.length()) {
                throw new HiveClientException("Invalid or Incomplete table properties details for hive table while deserialization");
            }
            tableProperties = createCommand.substring(tblPropertiesBlockStartIndex, tblPropertiesBlockEndIndex + 1);
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @Builder
    public static class TableColumn {
        private String columnName;
        private String columnType;

        public TableColumn() {

        }

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof TableColumn)) return false;
            TableColumn other = (TableColumn) o;
            if (this.getColumnName() == null ? other.getColumnName() != null :
                    !this.getColumnName().equals(other.getColumnName())) return false;

            return !(this.getColumnType() == null ? other.getColumnType() != null :
                        !this.getColumnType().equals(other.getColumnType()));
        }

    }
}
