package org.khdl06.lab02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class MyMapper extends Mapper<LongWritable, Text, IntWritable, IntPair> {
    private List<String> headers;

    @Override
    protected void setup(Mapper<LongWritable, Text, IntWritable, IntPair>.Context context) throws IOException, InterruptedException {
        headers = Utils.getMtxHeaders(context.getConfiguration());
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,
            IntWritable, IntPair>.Context context) throws IOException,
            InterruptedException {
        if (headers.contains(value.toString())) {
            return;
        }

        String[] parts = value.toString().split("\\s+");
        int tokenIdx = Integer.parseInt(parts[0]);
        int docIdx = Integer.parseInt(parts[1]);
        int freq = Integer.parseInt(parts[2]);

        context.write(
                new IntWritable(tokenIdx),
                new IntPair(docIdx, freq)
        );
    }
}
