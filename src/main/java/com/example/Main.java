package com.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.EnvironmentConfiguration;

import java.io.*;
import java.util.List;

@Slf4j
public class Main {

    final static S3Service s3Service = new S3Service();
    final static TaskQueueService taskQueueService = new TaskQueueService();
    final static String bucketName = "scalable-p2";

    final static List<String> command;
    final static String inputFileName;
    final static String outputFileName;
    final static String inputTaskQueueName;
    final static String outputTaskQueueName;
    final static String workerType;

    static {
        EnvironmentConfiguration config = new EnvironmentConfiguration();
        config.setThrowExceptionOnMissing(true);
        workerType = config.getString("WORKER_TYPE");

        inputTaskQueueName = workerType;
        if (workerType.equals("convert")) {
            inputFileName = "original";
            outputFileName = "convert.mp4";
            outputTaskQueueName = "thumbnail";
            command = List.of(
                    "ffmpeg", "-r", "24", "-i", inputFileName,
                    "-c:v", "libx264", "-g", "240", "-keyint_min", "0",
                    "-sc_threshold", "0", outputFileName
            );
        } else if (workerType.equals("thumbnail")) {
            inputFileName = "convert.mp4";
            outputFileName = "thumbnail.png";
            outputTaskQueueName = "backend";
            command = List.of(
                    "ffmpeg", "-i", inputFileName, "-frames:v", "1", outputFileName
            );
        } else {
            throw new RuntimeException("WORKER_TYPE must be convert, thumbnail, or chunk.");
        }
    }

    public static void main(String[] args) {
        while (true) {
            String fileNamePrefix = taskQueueService.getTask(inputTaskQueueName);

            s3Service.downloadFile(bucketName, fileNamePrefix + "/" + inputFileName, inputFileName);

            executeCommand(command);

            s3Service.putS3Object(bucketName, fileNamePrefix + "/" + outputFileName, outputFileName);

            taskQueueService.sendTask(outputTaskQueueName, fileNamePrefix);

            deleteFile(inputFileName);
            deleteFile(outputFileName);
        }
    }

    private static void executeCommand(List<String> command) {
        try {
            Process p = new ProcessBuilder(command).start();
            printInputStream(p.getErrorStream());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private static void printInputStream(InputStream in) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = br.readLine()) != null)
                System.out.println(line);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private static void deleteFile(String fileName) {
        File inputFile = new File(fileName);
        if (inputFile.delete())
            log.info("deleted " + fileName);
        else
            log.error("failed to delete " + fileName);
    }
}
