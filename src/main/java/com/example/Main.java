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
    final static String inputFileName;
    final static String outputFileName;
    final static String inputTaskQueueName;
    final static String outputTaskQueueName;
    final static String taskType;

    static {
        EnvironmentConfiguration config = new EnvironmentConfiguration();
        config.setThrowExceptionOnMissing(true);
        taskType = config.getString("TASK_TYPE");

        inputTaskQueueName = taskType;
        if (taskType.equals("convert")) {
            inputFileName = "original";
            outputFileName = "convert.mp4";
            outputTaskQueueName = "thumbnail";
        } else if (taskType.equals("thumbnail")) {
            inputFileName = "convert.mp4";
            outputFileName = "thumbnail.png";
            outputTaskQueueName = "backend";
        } else {
            throw new RuntimeException("TASK_TYPE must be convert, thumbnail, or chunk.");
        }
    }

    public static void main(String[] args) {
        while (true) {
            String fileNamePrefix = taskQueueService.getTask(inputTaskQueueName);

            s3Service.downloadFile(bucketName, fileNamePrefix + "/" + inputFileName, inputFileName);

            if (taskType.equals("convert")) {
                convertVideo(inputFileName, outputFileName);
            } else if (taskType.equals("thumbnail")) {
                createThumbnail(inputFileName, outputFileName);
            }

            s3Service.putS3Object(bucketName, fileNamePrefix + "/" + outputFileName, outputFileName);

            taskQueueService.sendTask(outputTaskQueueName, fileNamePrefix);

            deleteFile(inputFileName);
            deleteFile(outputFileName);
        }
    }

    private static void convertVideo(String inputFileName, String outputFileName) {
        try {
            Process p = new ProcessBuilder("ffmpeg", "-i", inputFileName, outputFileName).start();
            printInputStream(p.getErrorStream());
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private static void createThumbnail(String inputFileName, String outputFileName) {
        try {
            Process p = new ProcessBuilder("ffmpeg", "-i", inputFileName, "-frames:v", "1", outputFileName).start();
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
