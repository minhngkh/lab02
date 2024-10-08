package org.khdl06.lab02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class DiffTermPerDoc {
    public class MapperImpl extends Mapper<LongWritable, Text, IntWritable,
            NullWritable> {
        private List<String> headers;

        @Override
        protected void setup(Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            headers = Utils.getMtxHeaders(context.getConfiguration(), "tf2");
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,
                IntWritable, NullWritable>.Context context) throws IOException,
                InterruptedException {
            if (headers.contains(value.toString())) {
                return;
            }

            String[] parts = value.toString().split("\\s+");
            int tokenIdx = Integer.parseInt(parts[0]);
            int docIdx = Integer.parseInt(parts[1]);
            int freq = Integer.parseInt(parts[2]);

            context.write(new IntWritable(docIdx), NullWritable.get());
        }
    }

    public class ReducerImpl extends Reducer<IntWritable, NullWritable, IntWritable,
            IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values,
                              Reducer<IntWritable, NullWritable, IntWritable,
                                      IntWritable>.Context context) throws IOException,
                InterruptedException {
            int count = 0;
            for (NullWritable value : values) {
                ++count;
            }

            context.write(key, new IntWritable(count));
        }
    }
}
