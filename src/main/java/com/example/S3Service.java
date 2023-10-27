package com.example;

import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.nio.file.Paths;

public class S3Service {

    private S3TransferManager transferManager = S3TransferManager.create();

    /*
     * source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_GetObject_section.html
     */
    public Long downloadFile(String bucketName, String key, String downloadedFileWithPath) {
        DownloadFileRequest downloadFileRequest =
                DownloadFileRequest.builder()
                        .getObjectRequest(b -> b.bucket(bucketName).key(key))
                        .addTransferListener(LoggingTransferListener.create())
                        .destination(Paths.get(downloadedFileWithPath))
                        .build();

        FileDownload downloadFile = transferManager.downloadFile(downloadFileRequest);

        CompletedFileDownload downloadResult = downloadFile.completionFuture().join();
        return downloadResult.response().contentLength();
    }
}
