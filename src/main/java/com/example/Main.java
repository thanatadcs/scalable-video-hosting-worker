package com.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.EnvironmentConfiguration;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

@Slf4j
public class Main {

    final static TaskQueueService taskQueueService = new TaskQueueService();
    final static String bucketName = "scalable-p2";
    final static S3Service s3Service;
    final static List<String> command;
    final static String inputFileName;
    final static String outputFileName;
    final static String inputTaskQueueName;
    final static String[] outputTaskQueueNames;
    final static String workerType;
    final static Consumer<String> deleteFunction;
    final static String messagePrefix;

    static {
        EnvironmentConfiguration config = new EnvironmentConfiguration();
        config.setThrowExceptionOnMissing(true);
        workerType = config.getString("WORKER_TYPE");
        s3Service = new S3Service(bucketName, workerType);
        deleteFunction = workerType.equals("chunk") ? Main::cleanDirectory : Main::deleteFile;
        inputTaskQueueName = workerType;

        if (workerType.equals("convert")) {
            messagePrefix = "";
            inputFileName = "original";
            outputFileName = "convert.mp4";
            outputTaskQueueNames = new String[]{"thumbnail", "chunk"};
            command = List.of(
                    "ffmpeg", "-r", "24", "-i", inputFileName,
                    "-c:v", "libx264", "-g", "240", "-keyint_min", "0",
                    "-sc_threshold", "0", outputFileName
            );
        } else if (workerType.equals("thumbnail")) {
            messagePrefix = workerType + ",";
            inputFileName = "convert.mp4";
            outputFileName = "thumbnail.png";
            outputTaskQueueNames = new String[]{"backend"};
            command = List.of(
                    "ffmpeg", "-i", inputFileName, "-frames:v", "1", outputFileName
            );
        } else if (workerType.equals("chunk")) {
            messagePrefix = workerType + ",";
            inputFileName = "convert.mp4";
            outputFileName = "playlist";
            outputTaskQueueNames = new String[]{"backend"};
            command = List.of(
                    "ffmpeg", "-i", inputFileName, "-c:v", "copy",
                    "-start_number", "0", "-hls_time", "10", "-hls_list_size", "0",
                    "-f", "hls", outputFileName + "/" + outputFileName + ".m3u8"
            );
            new File(outputFileName).mkdir();
        } else {
            throw new RuntimeException("WORKER_TYPE must be convert, thumbnail, or chunk.");
        }
    }

    public static void main(String[] args) {
        while (true) {
            try {
                String fileNamePrefix = taskQueueService.getTask(inputTaskQueueName);

                s3Service.downloadFile(fileNamePrefix, inputFileName);

                executeCommand(command);

                s3Service.upload(fileNamePrefix, outputFileName);

                Arrays.stream(outputTaskQueueNames).forEach(
                        outputTaskQueueName ->
                                taskQueueService.sendTask(outputTaskQueueName, messagePrefix + fileNamePrefix)
                );

                deleteFile(inputFileName);
                delete(outputFileName);
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }

    private static void executeCommand(List<String> command) throws IOException {
        Process p = new ProcessBuilder(command).start();
        printInputStream(p.getErrorStream());
    }

    private static void printInputStream(InputStream in) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = br.readLine()) != null)
                System.out.println(line);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void delete(String fileName) {
        deleteFunction.accept(fileName);
    }

    private static void cleanDirectory(String dirName) {
        File dir = new File(dirName);
        for(File file: dir.listFiles()) {
            if (!file.isDirectory()) {
                file.delete();
                log.info("Deleted " + file.getName());
            }
        }
    }

    private static void deleteFile(String fileName) {
        File inputFile = new File(fileName);
        if (inputFile.delete())
            log.info("Deleted " + fileName);
        else
            log.error("Failed to delete " + fileName);
    }

}
