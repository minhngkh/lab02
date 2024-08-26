package org.khdl06.lab02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Reducer3 extends Reducer<IntWritable, IntWritable, IntWritable,
        IntWritable> {
    private TreeMap<Integer, Integer> map;

    @Override
    protected void setup(Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        map = new TreeMap<>();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values,
                          Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            map.put(key.get(), value.get());
            if (map.size() > 10) {
                map.remove(map.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {

        for (Map.Entry<Integer, Integer> entry : map.descendingMap().entrySet()) {
            context.write(new IntWritable(entry.getKey()),
                    new IntWritable(entry.getValue()));
        }
    }
}
