package com.flipkart.dsp.sg.jobs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Iterator;

import static com.flipkart.dsp.utils.Constants.DATAFRAME_KEY;

public class PartitioningFileReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> mos;
    private String dataframeId;

    @Override
    protected void setup(Context context) throws IOException {
        mos = new MultipleOutputs(context);
        dataframeId = context.getConfiguration().get(DATAFRAME_KEY);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator iterator = values.iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            Text text = (Text) iterator.next();
            mos.write(dataframeId, null, text, key.toString());
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
