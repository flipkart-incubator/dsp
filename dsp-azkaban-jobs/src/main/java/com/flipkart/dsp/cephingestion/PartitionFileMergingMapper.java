package com.flipkart.dsp.cephingestion;

import com.flipkart.dsp.exceptions.CephIngestionException;
import com.flipkart.dsp.utils.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.flipkart.dsp.utils.Constants.*;

/**
 * +
 */
@Slf4j
public class PartitionFileMergingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String fieldDelimiter;

    @Override
    protected void setup(Context context) {
        fieldDelimiter = context.getConfiguration().get(FIELD_DELIMITER);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException {
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(value.toString()))))) {
            br.lines().forEach(line -> {
                try {
                    String rowValue = generateRowValue(line, value.toString());
                    context.write(new Text(""), new Text(rowValue));
                } catch (Exception e) {
                    throw new CephIngestionException("Error while running mapper while merging file for Ceph Upload: ErrorMessage - " + e.getMessage());
                }
            });
        }
    }

    private String generateRowValue(String line, String fileName) {
        String[] rowData = line.split(fieldDelimiter);
        List<String> csvRowData = new ArrayList<>(Arrays.asList(rowData));
        String dirFromRefreshId = fileName.substring(fileName.indexOf(Constants.REFRESH_ID), fileName.lastIndexOf(slash));
        List<String> partitionColumnMappings = new ArrayList<>(Arrays.asList(dirFromRefreshId.split(slash)));
        String refreshId = partitionColumnMappings.get(0).split(equal)[1];
        partitionColumnMappings.remove(0);
        partitionColumnMappings.forEach(v -> csvRowData.add(v.split(equal)[1]));
        csvRowData.add(refreshId);
        return String.join(fieldDelimiter, csvRowData);
    }
}
