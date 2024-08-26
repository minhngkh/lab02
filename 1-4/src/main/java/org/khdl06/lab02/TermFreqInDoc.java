package org.khdl06.lab02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class TermFreqInDoc {
    public class MapperImpl extends Mapper<LongWritable, Text, IntPair,
            IntWritable> {
        private List<String> headers;

        @Override
        protected void setup(Mapper<LongWritable, Text, IntPair, IntWritable>.Context context) throws IOException, InterruptedException {
            headers = Utils.getMtxHeaders(context.getConfiguration());
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,
                IntPair, IntWritable>.Context context) throws IOException,
                InterruptedException {
            if (headers.contains(value.toString())) {
                return;
            }

            String[] parts = value.toString().split("\\s+");
            int tokenIdx = Integer.parseInt(parts[0]);
            int docIdx = Integer.parseInt(parts[1]);
            int freq = Integer.parseInt(parts[2]);

            context.write(
                    new IntPair(tokenIdx, docIdx),
                    new IntWritable(freq)
            );
        }
    }

    public class ReducerImpl extends Reducer<IntPair, IntWritable, IntPair, IntWritable> {
        @Override
        protected void reduce(IntPair key, Iterable<IntWritable> values,
                              Reducer<IntPair, IntWritable, IntPair, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }
}