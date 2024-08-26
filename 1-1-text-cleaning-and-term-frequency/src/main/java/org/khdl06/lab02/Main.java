package org.khdl06.lab02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException,
            ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "1.1");

        Utils utils = new Utils(conf, args[0], args[1], args[2]);
        utils.trashTempTaskFile();
        utils.addStopWordsFileToCache(job);
        utils.configureJobIoFile(job);

        job.setJarByClass(Main.class);
        job.setMapperClass(TokenMapper.class);
        job.setCombinerClass(FrequencyReducer.class);
        job.setCombinerClass(FrequencyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(DocumentToken.class);
        job.setOutputValueClass(IntWritable.class);

        int exitCode = job.waitForCompletion(true) ? 0 : 1;
        if (exitCode != 0) {
            System.exit(exitCode);
        }

        utils.exportToMtx();
        System.exit(0);
    }
}
