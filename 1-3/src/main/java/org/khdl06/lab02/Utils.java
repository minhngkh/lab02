package org.khdl06.lab02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Utils {
    private final Configuration Conf;
    private final String ConfigDirectory;
    private final String InputFile;
    private final String OutputDirectory;

    public static final String TempName = "temp-1-3";


    public Utils(Configuration conf, String configDirectory, String inputFile,
                 String outputDirectory) {
        this.Conf = conf;
        this.ConfigDirectory = configDirectory;
        this.InputFile = inputFile;
        this.OutputDirectory = outputDirectory;
    }

    public static List<String> getMtxHeaders(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        return CoreUtils.getMtxHeaders(fs, TempName);
    }

    public void configureJobIoFile(Job job) throws IOException {
        FileInputFormat.addInputPath(job, new Path(InputFile));
        FileOutputFormat.setOutputPath(job,
                CoreUtils.getTaskTempDirectory(TempName));
    }

    public void trashTempTaskFile() throws IOException {
        FileSystem fs = FileSystem.get(Conf);
        fs.delete(CoreUtils.getTaskTempDirectory(TempName), true);
    }

    public void processInputMtxInfo(Job job) throws IOException {
        CoreUtils.processInputMtxInfo(job, InputFile, TempName);
    }

    public void exportToTxtFile() throws IOException {
        FileSystem fs = FileSystem.get(Conf);


        FileStatus[] tempOutputFiles = fs.listStatus(
                CoreUtils.getTaskTempDirectory(TempName)
        );
        for (FileStatus file : tempOutputFiles) {
            Path curPath = file.getPath();
            if (!curPath.getName().startsWith("part-r-")) {
                continue;
            }

            List<String> lookup = CoreUtils.getTermLookup(fs, ConfigDirectory);

            ArrayList<String[]> data = new ArrayList<>();
            try (BufferedReader reader = CoreUtils.getReader(fs, curPath)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split("\\s+");
                    int termIdx = Integer.parseInt(tokens[1]);
                    String freq = tokens[0];

                    data.add(new String[]{lookup.get(termIdx), freq});
                }

                Path outputPath = new Path(OutputDirectory + "/" + "task_1_3.txt");
                try (FSDataOutputStream writer = CoreUtils.getWriter(fs, outputPath)) {
                    for (String[] d : data) {
                        writer.writeBytes(d[0] + ": " + d[1] + "\n");
                    }
                }

            }
        }
    }
}
