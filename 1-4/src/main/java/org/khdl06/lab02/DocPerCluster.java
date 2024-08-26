package org.khdl06.lab02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DocPerCluster {
    public class MapperImpl extends Mapper<LongWritable, Text, Text,
            IntWritable> {
        private List<String> headers;
        private Map<Integer, String> clusterMap;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            headers = Utils.getMtxHeaders(context.getConfiguration());
            clusterMap = Utils.getClusterMap(context.getConfiguration());
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,
                Text, IntWritable>.Context context) throws IOException,
                InterruptedException {
            if (headers.contains(value.toString())) return;

            String[] parts = value.toString().split("\\s+");
            int tokenIdx = Integer.parseInt(parts[0]);
            int docIdx = Integer.parseInt(parts[1]);
            int freq = Integer.parseInt(parts[2]);

            context.write(new Text(clusterMap.get(docIdx)), new IntWritable(docIdx));
        }
    }

    public class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text,
                IntWritable, Text, IntWritable>.Context context) throws IOException,
                InterruptedException {
            Set<Integer> docIdxSet = new HashSet<>();

            for (IntWritable value : values) {
                docIdxSet.add(value.get());
            }

            context.write(key, new IntWritable(docIdxSet.size()));
        }
    }
}
