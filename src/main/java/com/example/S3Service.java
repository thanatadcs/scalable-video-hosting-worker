package com.example;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.*;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.nio.file.Paths;

@Slf4j
public class S3Service {

    private S3TransferManager transferManager;

    S3Service() {
        transferManager = S3TransferManager.create();
    }

    /*
     * Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_GetObject_section.html
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

    /*
     * Source: https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/transfer/s3/S3TransferManager.html
     */
    public void uploadFile(String bucketName, String objectKey, String objectPath) {
        UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                .putObjectRequest(req -> req.bucket(bucketName).key(objectKey))
                .addTransferListener(LoggingTransferListener.create())
                .source(Paths.get(objectPath))
                .build();

        FileUpload upload = transferManager.uploadFile(uploadFileRequest);
        upload.completionFuture().join();
    }

    /*
     * Source: https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/transfer/s3/S3TransferManager.html
     */
    public Integer uploadDirectory(String sourceDirectory, String bucketName){
        DirectoryUpload directoryUpload =
                transferManager.uploadDirectory(UploadDirectoryRequest.builder()
                        .source(Paths.get(sourceDirectory))
                        .bucket(bucketName)
                        .build());

        CompletedDirectoryUpload completedDirectoryUpload = directoryUpload.completionFuture().join();
        completedDirectoryUpload.failedTransfers().forEach(fail ->
                log.warn("Object [{}] failed to transfer", fail.toString()));
        return completedDirectoryUpload.failedTransfers().size();
    }


}
