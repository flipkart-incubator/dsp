package com.flipkart.dsp.sg.jobs;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.utils.Constants.*;

@Slf4j
public class PartitioningFileMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final String DELIMITER = "#";
    private String fieldDelimiter;
    private List<String> headers;
    private List<String> partitionColumns;

    @Override
    protected void setup(Context context) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        fieldDelimiter = context.getConfiguration().get(FIELD_DELIMITER);
        headers = objectMapper.readValue(context.getConfiguration().get(HEADERS_KEY), new TypeReference<List<String>>() {
        });
        partitionColumns = objectMapper.readValue(context.getConfiguration().get(PARTITION_COLUMNS), new TypeReference<List<String>>() {
        });
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws InterruptedException {
        try {
            String[] rowData = value.toString().split(fieldDelimiter);
            String partitionKey = generateKey(rowData);
            String rowValue = generateRowValue(rowData);
            context.write(new Text(partitionKey), new Text(rowValue));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String generateKey(String[] rowData) {
        StringBuilder partitionKey = new StringBuilder();
        for (String column : partitionColumns) {
            if (headers.contains(column)) {
                partitionKey.append(rowData[headers.indexOf(column)]);
                partitionKey.append(DELIMITER);
            }
        }
        partitionKey.deleteCharAt(partitionKey.lastIndexOf(DELIMITER));
        return partitionKey.toString();
    }

    private String generateRowValue(String[] rowData) {
        List<String> csvRowData = new ArrayList<>();
        for (String column : headers) {
            if (!partitionColumns.contains(column)) {
                csvRowData.add(rowData[headers.indexOf(column)]);
            }
        }
        return String.join(fieldDelimiter, csvRowData);
    }
}
