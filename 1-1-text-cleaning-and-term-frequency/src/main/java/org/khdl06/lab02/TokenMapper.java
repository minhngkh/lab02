package org.khdl06.lab02;

import org.apache.commons.compress.utils.FileNameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Set;

public class TokenMapper extends Mapper<LongWritable, Text, DocumentToken, IntWritable> {
    private Set<String> stopWords;

    @Override
    protected void setup(Mapper<LongWritable, Text, DocumentToken, IntWritable>.Context context) throws IOException {
        stopWords = Utils.getStopWords(context.getConfiguration());
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,
            DocumentToken, IntWritable>.Context context) throws IOException,
            InterruptedException {

        String[] tokens = value.toString().split("\\s+");

        for (String token : tokens) {
            // Remove punctuations at the end of the token
            token = token.toLowerCase().replaceAll("\\p{Punct}+$", "");

            // Check if the token contains only letters, digits
            // or the £ symbol (but not for things like $ according to bbc.terms ?)
            if (token.matches("^[a-z0-9£]+$")) {
                FileSplit split = (FileSplit) context.getInputSplit();
                Path filePath = split.getPath();

                String fileName = FileNameUtils.getBaseName(filePath.getName());
                String fileDirectory = filePath.getParent().getName();

                context.write(
                        new DocumentToken(token, fileName, fileDirectory),
                        new IntWritable(1)
                );
            }
        }
    }
}
