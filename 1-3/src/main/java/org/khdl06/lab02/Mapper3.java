package org.khdl06.lab02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Mapper3 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private TreeMap<Integer, Integer> map;
    private List<String> headers;

    @Override
    protected void setup(Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        map = new TreeMap<>();
        headers = Utils.getMtxHeaders(context.getConfiguration());

    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,
            IntWritable, IntWritable>.Context context) {
        if (headers.contains(value.toString())) {
            return;
        }

        String[] tokens = value.toString().split("\\s+");
        int termIdx = Integer.parseInt(tokens[0]);
        int docIdx = Integer.parseInt(tokens[1]);
        int freq = Integer.parseInt(tokens[2]);

        map.put(freq, termIdx);

        if (map.size() > 10) {
            map.remove(map.firstKey());
        }
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            context.write(
                    new IntWritable(entry.getKey()),
                    new IntWritable(entry.getValue())
            );
        }
    }
}
