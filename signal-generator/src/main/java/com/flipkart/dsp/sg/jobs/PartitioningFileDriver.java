package com.flipkart.dsp.sg.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.util.List;

import static com.flipkart.dsp.utils.Constants.*;

@Slf4j
@AllArgsConstructor
public class PartitioningFileDriver extends Configured implements Tool {
    String dataFrameId;
    String fieldDelimiter;
    List<String> headers;
    List<String> partitionColumns;

    public int run(String[] args) throws Exception {
        log.info("mr job initiated");
        String sourcePath = args[0];
        String destinationPath = args[1];
        ObjectMapper objectMapper = new ObjectMapper();
        Configuration conf = getConf();
        conf.set(DATAFRAME_KEY, dataFrameId);
        conf.set(FIELD_DELIMITER, fieldDelimiter);
        conf.set(HEADERS_KEY, objectMapper.writeValueAsString(headers));
        conf.set(PARTITION_COLUMNS, objectMapper.writeValueAsString(partitionColumns));

        Job job = Job.getInstance(conf, getClass().getName());

        job.setJarByClass(getClass());
        job.setMapperClass(PartitioningFileMapper.class);
        job.setReducerClass(PartitioningFileReducer.class);

        Path inputPath = new Path(sourcePath);
        Path outputPath = new Path(destinationPath);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, outputPath);
        FileInputFormat.addInputPath(job, inputPath);
        MultipleOutputs.addNamedOutput(job, dataFrameId, TextOutputFormat.class, Text.class, Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
