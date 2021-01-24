package com.flipkart.dsp.cephingestion;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static com.flipkart.dsp.utils.Constants.*;

/**
 * +
 */
@Slf4j
@AllArgsConstructor
public class PartitionFileMergingDriver extends Configured implements Tool{
    List<String> files;
    public int run(String[] args) throws Exception {
        log.debug("MR job initiated for merging HDFS file for Ceph upload");
        String destinationPath = args[0];
        String separator = args[1];

        Configuration configuration = getConf();
        configuration.set(FIELD_DELIMITER, separator);
        configuration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

        String tempDir = getTempDir();
        Path inputPath = new Path(tempDir, "input");

        createInputFile(inputPath, configuration);
        Path outputPath = new Path(destinationPath);

        Job job = Job.getInstance(configuration, getClass().getName());
        job.setJarByClass(getClass());
        job.setMapperClass(PartitionFileMergingMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private String getTempDir() {
        return HDFS_CLUSTER_PREFIX + HADOOP_CLUSTER + slash + tmp + slash + UUID.randomUUID();
    }

    private void createInputFile(Path inputDir, Configuration configuration) throws IOException {
        FileSystem fileSystem = inputDir.getFileSystem(configuration);
        Path path = new Path(inputDir, UUID.randomUUID().toString());
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, configuration, path, LongWritable.class,
                Text.class, SequenceFile.CompressionType.NONE);

        for (int i=0; i < files.size(); i++) {
            writer.append(new LongWritable(i), new Text(files.get(i)));
        }
        writer.close();
    }
}

