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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {
    private final Configuration Conf;
    private final String ConfigDirectory;
    private final String InputFile;
    private final String OutputDirectory;

    public static final String TempName = "temp-1-2";


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

        Conf.set("configDirectory", ConfigDirectory);
    }

    public void trashTempTaskFile(String jobName) throws IOException {
        FileSystem fs = FileSystem.get(Conf);
        fs.delete(CoreUtils.getTaskTempDirectory(TempName + "/" + jobName), true);
    }

    public void processInputMtxInfo(Job job, ) throws IOException {
        CoreUtils.processInputMtxInfo(job, InputFile, TempName);
    }

    public void exportToMtx() throws IOException {
        FileSystem fs = FileSystem.get(Conf);

        // Read .terms and .docs files
        int numTerms = CoreUtils.getTermIdxMap(fs, ConfigDirectory).size();
        int numDocs = CoreUtils.getDocIdxMap(fs, ConfigDirectory).size();

        // Read data from temp files
        int entries = 0;
        FileStatus[] tempOutputFiles = fs.listStatus(
                CoreUtils.getTaskTempDirectory(TempName)
        );

        ArrayList<int[]> data = new ArrayList<>();
        for (FileStatus file : tempOutputFiles) {
            Path curPath = file.getPath();
            if (!curPath.getName().startsWith("part-r-")) {
                continue;
            }

            try (BufferedReader reader = CoreUtils.getReader(fs, curPath)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    int termIdx = Integer.parseInt(parts[0]);
                    int docIdx = Integer.parseInt(parts[1]);
                    int frequency = Integer.parseInt(parts[2]);

                    data.add(new int[]{termIdx, docIdx, frequency});
                    ++entries;
                }
            }
        }

        // Start crafting the output file
        Path outputPath = new Path(OutputDirectory + "/task_1_2.mtx");
        try (FSDataOutputStream writer = CoreUtils.getWriter(fs, outputPath)) {
            // Write header line
            writer.writeBytes("%%MatrixMarket matrix coordinate real general\n");
            writer.writeBytes(numTerms + " " + numDocs + " " + entries + "\n");

            for (int[] d : data) {
                writer.writeBytes(d[0] + " " + d[1] + " " + d[2] + "\n");
            }
        }
    }


    public static Map<Integer, String> getClusterMap(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        String configDirectory = conf.get("configDirectory");
        Path docsFile = new Path(configDirectory + "/" + CoreUtils.DocsFileName);

        try (BufferedReader reader = CoreUtils.getReader(fs, docsFile)) {
            HashMap<Integer, String> map = new HashMap<>();

            int idx = 1;
            String str;
            while ((str = reader.readLine()) != null) {
                map.put(idx, str.split("\\.")[0]);
                ++idx;
            }

            return map;
        }
    }

}
