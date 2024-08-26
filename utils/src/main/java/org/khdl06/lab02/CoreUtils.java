package org.khdl06.lab02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;
import java.util.*;

public class CoreUtils {
    public static final String StopWordsFileName = "stopwords.txt";
    public static final String TermsFileName = "bbc.terms";
    public static final String DocsFileName = "bbc.docs";
    public static final String HeaderDirectory = "header";
    private static final String TempDirectory = ".tmp";

    private static Map<String, Integer> getStringIntegerMap(FileSystem fs,
                                                            String dir,
                                                            String fileName) throws IOException {
        Path path = new Path(dir + "/" + fileName);
        try (BufferedReader reader = getReader(fs, path)) {
            HashMap<String, Integer> map = new HashMap<>();

            int idx = 1;
            String str;
            while ((str = reader.readLine()) != null) {
                map.put(str, idx);
                ++idx;
            }

            return map;
        }
    }

    private static List<String> getStringLookup(FileSystem fs, String configDirectory,
                                                String fileName) throws IOException {
        Path path = new Path(configDirectory + "/" + fileName);
        try (BufferedReader reader = getReader(fs, path)) {
            List<String> list = new ArrayList<>();
            list.add(null);

            String str;
            while ((str = reader.readLine()) != null) {
                list.add(str);
            }

            return list;
        }
    }

    public static void insertLine(FileSystem fs, Path path, String newLine,
                                  int position) throws IOException {
        Path tempPath = new Path(path.toString() + ".tmp");

        System.out.println("Temp path: " + tempPath.toString());

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(fs.open(path)));
             FSDataOutputStream writer = getWriter(fs, tempPath)) {

            String currentLine;
            int lineNumber = 0;
            while ((currentLine = reader.readLine()) != null) {
                if (lineNumber == position) {
                    writer.writeBytes(newLine);
                    writer.writeByte('\n');
                }
                writer.writeBytes(currentLine);
                writer.writeByte('\n');
                ++lineNumber;
            }

            if (position >= lineNumber) {
                writer.writeBytes(newLine);
                writer.writeByte('\n');
            }
        }

        if (!fs.delete(path, false)) {
            throw new IOException("Could not delete original file");
        }
        if (!fs.rename(tempPath, path)) {
            throw new IOException("Could not rename temporary file");
        }
    }

    public static Path getTaskTempDirectory(String name) {
        return new Path(TempDirectory + "/" + name);
    }

    public static BufferedReader getReader(FileSystem fs, Path path) throws IOException {
        FSDataInputStream is = fs.open(path);

        return new BufferedReader(new InputStreamReader(is));
    }

    public static FSDataOutputStream getWriter(FileSystem fs, Path path) throws IOException {
        return fs.create(path);
    }

    public static Map<String, Integer> getTermIdxMap(FileSystem fs,
                                                     String configDirectory) throws IOException {
        return getStringIntegerMap(fs, configDirectory, TermsFileName);
    }

    public static Map<String, Integer> getDocIdxMap(FileSystem fs,
                                                    String configDirectory) throws IOException {
        return getStringIntegerMap(fs, configDirectory, DocsFileName);
    }

    public static List<String> getTermLookup(FileSystem fs, String configDirectory) throws IOException {
        return getStringLookup(fs, configDirectory, TermsFileName);
    }

    public static List<String> getDocLookup(FileSystem fs, String configDirectory) throws IOException {
        return getStringLookup(fs, configDirectory, DocsFileName);
    }


    public static void processInputMtxInfo(Job job, String inputFilePath,
                                           String taskTempName) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());

        Path inputPath = new Path(inputFilePath);
        Path headerPath = new Path(
                getTaskTempDirectory(HeaderDirectory) + "/" + taskTempName
        );

        try (BufferedReader reader = getReader(fs, inputPath);
             FSDataOutputStream headerWriter = getWriter(fs, headerPath)) {

            String line;
            while ((line = reader.readLine()) != null) {
                headerWriter.writeBytes(line);
                headerWriter.writeByte('\n');

                if (!line.startsWith("%")) {
                    break;
                }
            }
        }

        job.addCacheFile(headerPath.toUri());
    }

    public static List<String> getMtxHeaders(FileSystem fs, String taskTempName) throws IOException {

        Path headerPath = new Path(
                CoreUtils.getTaskTempDirectory(HeaderDirectory) + "/" + taskTempName
        );

        try (BufferedReader reader = CoreUtils.getReader(fs, headerPath)) {
            List<String> headers = new ArrayList<>();

            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                headers.add(currentLine);
            }

            return headers;
        }
    }
}
