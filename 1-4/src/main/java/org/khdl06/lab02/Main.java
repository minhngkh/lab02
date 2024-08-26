package org.khdl06.lab02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException,
            ClassNotFoundException {
        Configuration conf = new Configuration();


        Utils utils = new Utils(conf, args[0], args[1], args[2]);


        Job tfJob1 = Job.getInstance(conf, "TermFreqInDoc");
        tfJob1.setCombinerClass(TermFreqInDoc.ReducerImpl.class);
        tfJob1.setReducerClass(TermFreqInDoc.ReducerImpl.class);
        tfJob1.setOutputKeyClass(IntPair.class);
        tfJob1.setOutputValueClass(IntWritable.class);
        utils.configureJobIoFile(tfJob1, "tf1");
        utils.processInputMtxInfo(tfJob1, "same");

        Job tfJob2 = Job.getInstance(conf, "DiffTermPerDoc");
        tfJob2.setCombinerClass(DiffTermPerDoc.ReducerImpl.class);
        tfJob2.setReducerClass(DiffTermPerDoc.ReducerImpl.class);
        tfJob2.setOutputKeyClass(IntWritable.class);
        tfJob2.setOutputValueClass(IntWritable.class);
        utils.configureJobIoFile(tfJob1, "tf2");
        utils.processInputMtxInfo(tfJob1, "same");


        Job idfJob1 = Job.getInstance(conf, "DocPerCluster");
        idfJob1.setCombinerClass(DocPerCluster.ReducerImpl.class);
        idfJob1.setReducerClass(DocPerCluster.ReducerImpl.class);
        idfJob1.setOutputKeyClass(Text.class);
        idfJob1.setOutputValueClass(IntWritable.class);
        utils.configureJobIoFile(tfJob1, "idf1");
        utils.processInputMtxInfo(tfJob1, "same");

        Job idfJob2 = Job.getInstance(conf, "DocContainingTermPerCluster");
        idfJob2.setCombinerClass(DocContainingTermPerCluster.ReducerImpl.class);
        idfJob2.setReducerClass(DocContainingTermPerCluster.ReducerImpl.class);
        idfJob2.setOutputKeyClass(IntStringPair.class);
        idfJob2.setOutputValueClass(IntWritable.class);
        utils.configureJobIoFile(tfJob1, "idf2");
        utils.processInputMtxInfo(tfJob1, "same");

        tfJob1.submit();
        idfJob1.submit();
        idfJob2.submit();

        if (tfJob1.waitForCompletion(true)) System.exit(1);
        tfJob2.submit();
        if (idfJob1.waitForCompletion(true)) System.exit(1);
        if (idfJob2.waitForCompletion(true)) System.exit(1);


        System.out.println("Exporting to .mtx file...");

        utils.exportToMtx();
        System.exit(0);
    }

}
