package com.example;

import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.*;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;

import java.nio.file.Paths;
import java.util.function.BiConsumer;

@Slf4j
public class S3Service {

    private S3TransferManager transferManager;
    private String bucketName;
    private BiConsumer<String, String> uploadFunction;
    private final String workerTypeForDirectoryUpload = "chunk";

    S3Service(String bucketName, String workerType) {
        transferManager = S3TransferManager.create();
        this.bucketName = bucketName;
        this.uploadFunction = workerType.equals(workerTypeForDirectoryUpload) ? this::uploadDirectory : this::uploadFile;
    }

    /*
     * Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/example_s3_GetObject_section.html
     */
    public Long downloadFile(String keyPrefix, String fileName) {
        DownloadFileRequest downloadFileRequest =
                DownloadFileRequest.builder()
                        .getObjectRequest(b -> b.bucket(bucketName).key(keyPrefix + "/" + fileName))
                        .addTransferListener(LoggingTransferListener.create())
                        .destination(Paths.get(fileName))
                        .build();

        FileDownload downloadFile = transferManager.downloadFile(downloadFileRequest);

        CompletedFileDownload downloadResult = downloadFile.completionFuture().join();
        return downloadResult.response().contentLength();
    }

    public void upload(String keyPrefix, String fileName) {
        uploadFunction.accept(keyPrefix, fileName);
    }

    /*
     * Source: https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/transfer/s3/S3TransferManager.html
     */
    public void uploadFile(String keyPrefix, String fileName) {
        UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                .putObjectRequest(req -> req.bucket(bucketName).key(keyPrefix + "/" + fileName))
                .addTransferListener(LoggingTransferListener.create())
                .source(Paths.get(fileName))
                .build();

        FileUpload upload = transferManager.uploadFile(uploadFileRequest);
        upload.completionFuture().join();
    }

    /*
     * Source: https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/transfer/s3/S3TransferManager.html
     */
    public void uploadDirectory(String keyPrefix, String sourceDirectory){
        DirectoryUpload directoryUpload =
                transferManager.uploadDirectory(UploadDirectoryRequest.builder()
                        .s3Prefix(keyPrefix + "/" + sourceDirectory)
                        .source(Paths.get(sourceDirectory))
                        .bucket(bucketName)
                        .build());

        CompletedDirectoryUpload completedDirectoryUpload = directoryUpload.completionFuture().join();
        completedDirectoryUpload.failedTransfers().forEach(fail ->
                log.warn("Object [{}] failed to transfer", fail.toString()));
    }

}
