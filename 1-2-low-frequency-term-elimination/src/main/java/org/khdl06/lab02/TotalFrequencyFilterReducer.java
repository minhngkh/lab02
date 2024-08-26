package org.khdl06.lab02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TotalFrequencyFilterReducer extends Reducer<IntWritable, IntPair,
        IntWritable, IntPair> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntPair> values,
                          Reducer<IntWritable, IntPair, IntWritable, IntPair>.Context context) throws IOException,
            InterruptedException {
        ArrayList<IntPair> valueList = new ArrayList<>();

        int sum = 0;
        for (IntPair value : values) {
            int freq = value.getSecond();
            sum += freq;

            valueList.add(new IntPair(value.getFirst(), freq));
        }

        if (sum < 3) return;

        for (IntPair intPair : valueList) {
            context.write(key, intPair);
        }
    }
}
