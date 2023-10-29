package com.example;

import java.io.*;

public class Main {
    final static S3Service s3Service = new S3Service();
    final static TaskQueueService taskQueueService = new TaskQueueService();
    final static String bucketName = "scalable-p2";
    final static String inputFileName = "original";
    final static String outputFileName = "convert.mp4";

    public static void main(String[] args) throws IOException {
        while (true) {
            String fileNamePrefix = taskQueueService.getTask();

            s3Service.downloadFile(bucketName, fileNamePrefix + inputFileName, inputFileName);

            convertVideo(inputFileName, outputFileName);

            deleteFile(inputFileName);
            deleteFile(outputFileName);
        }
    }

    private static void convertVideo(String inputFileName, String outputFileName) throws IOException {
        Process p = new ProcessBuilder("ffmpeg", "-i", inputFileName, outputFileName).start();
        InputStream in = p.getErrorStream();
        InputStreamReader inr = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(inr);
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
    }

    private static void deleteFile(String fileName) {
        File inputFile = new File(fileName);
        if (inputFile.delete()) {
            System.out.println("deleted" + fileName);
        } else {
            System.out.println("failed to delete" + fileName);
        }
    }
}
