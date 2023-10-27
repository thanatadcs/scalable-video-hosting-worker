package com.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Main {
    static S3Service s3Service = new S3Service();

    public static void main(String[] args) throws IOException {
        while (true) {
            TaskQueueService taskQueueService = new TaskQueueService();
            String fileName = taskQueueService.getTask();
            System.out.println(fileName);
            String downloadPath = "original";
            s3Service.downloadFile("scalable-p2", fileName + "/original", downloadPath);
            Process p = new ProcessBuilder("ffmpeg", "-i", downloadPath, "convert.mp4").start();
            InputStream in = p.getErrorStream();
            InputStreamReader inr = new InputStreamReader(in);
            BufferedReader br = new BufferedReader(inr);
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }

        }
    }
}
