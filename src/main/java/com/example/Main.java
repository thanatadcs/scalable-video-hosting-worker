package com.example;

import lombok.extern.slf4j.Slf4j;

import java.io.*;

@Slf4j
public class Main {
    final static S3Service s3Service = new S3Service();
    final static TaskQueueService taskQueueService = new TaskQueueService();
    final static String bucketName = "scalable-p2";
    final static String inputFileName = "original";
    final static String outputFileName = "convert.mp4";

    public static void main(String[] args) {
        while (true) {
            String fileNamePrefix = taskQueueService.getTask();

            s3Service.downloadFile(bucketName, fileNamePrefix + "/" + inputFileName, inputFileName);

            convertVideo(inputFileName, outputFileName);

            s3Service.putS3Object(bucketName, fileNamePrefix + "/" + outputFileName, outputFileName);

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
