package org.khdl06.lab02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;

public class Utils {
    private final Configuration Conf;
    private final String ConfigDirectory;
    private final String InputDirectory;
    private final String OutputDirectory;

    public static final String TempSubdirectory = "temp-1-1";


    public Utils(Configuration conf, String configDirectory, String inputDirectory,
                 String outputDirectory) throws IOException {
        this.Conf = conf;
        this.ConfigDirectory = configDirectory;
        this.InputDirectory = inputDirectory;
        this.OutputDirectory = outputDirectory;
    }

    public static Set<String> getStopWords(Configuration conf) throws IOException {
        Set<String> stopWords = new HashSet<>();

        FileSystem fs = FileSystem.get(conf);
        try (BufferedReader reader = CoreUtils.getReader(
                fs,
                new Path(CoreUtils.StopWordsFileName))
        ) {
            String word;
            while ((word = reader.readLine()) != null) {
                stopWords.add(word);
            }
        }

        return stopWords;
    }

    public void addStopWordsFileToCache(Job job) {
        job.addCacheFile(
                new Path(ConfigDirectory + "/" + CoreUtils.StopWordsFileName).toUri()
        );
    }

    public void exportToMtx() throws IOException {
        FileSystem fs = FileSystem.get(Conf);

        // Read .terms and .docs files
        Map<String, Integer> termMap = CoreUtils.getTermIdxMap(fs, ConfigDirectory);
        Map<String, Integer> docMap = CoreUtils.getDocIdxMap(fs, ConfigDirectory);

        // Start crafting the output file
        Path outputPath = new Path(OutputDirectory + "/task_1_1.mtx");
        try (FSDataOutputStream writer = CoreUtils.getWriter(fs, outputPath)) {

            // Write header line
            writer.writeBytes("%%MatrixMarket matrix coordinate real general\n");

            // Write data lines
            long entries = 0;
            HashSet<String> discardedTerms = new HashSet<>();
            FileStatus[] tempOutputFiles = fs.listStatus(
                    CoreUtils.getTaskTempDirectory(TempSubdirectory)
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
                        int termIdx = termMap.getOrDefault(parts[0], -1);
                        int docIdx = docMap.get(parts[1]);
                        int frequency = Integer.parseInt(parts[2]);

                        if (termIdx == -1) {
                            discardedTerms.add(parts[0]);
                            continue;
                        }

//                        writer.writeBytes(termIdx + " " + docIdx + " " + frequency +
//                                "\n");

                        data.add(new int[]{termIdx, docIdx, frequency});
                        ++entries;
                    }
                }
            }

            String sizeLine = termMap.size() + " " + docMap.size() + " " + entries;
            writer.writeBytes(sizeLine + "\n");
            for (int[] d : data) {
                writer.writeBytes(d[0] + " " + d[1] + " " + d[2] + "\n");
            }

            System.out.println("Discarded terms: " + discardedTerms.size());

            // Insert the size line
//            String sizeLine = termMap.size() + " " + docMap.size() + " " + entries;
//            CoreUtils.insertLine(fs, outputPath, sizeLine, 1);
        }
    }

    public void configureJobIoFile(Job job) throws IOException {
        FileInputFormat.addInputPath(job, new Path(InputDirectory));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job,
                CoreUtils.getTaskTempDirectory(TempSubdirectory));
    }

    public void trashTempTaskFile() throws IOException {
        FileSystem fs = FileSystem.get(Conf);
        fs.delete(CoreUtils.getTaskTempDirectory(TempSubdirectory), true);
    }
}
