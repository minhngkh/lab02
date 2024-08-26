package org.khdl06.lab02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FrequencyReducer extends Reducer<DocumentToken, IntWritable, DocumentToken
        , IntWritable> {
    @Override
    protected void reduce(DocumentToken key, Iterable<IntWritable> values,
                          Reducer<DocumentToken,
            IntWritable, DocumentToken, IntWritable>.Context context) throws IOException,
            InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
