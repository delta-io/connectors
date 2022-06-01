package io.delta.flink.e2e.utils;

import java.io.File;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

public class AwsUtils {

    public static void uploadDirectoryToS3(String targetBucketName,
                                           String s3Prefix,
                                           String localPath) throws InterruptedException {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        TransferManager manager = TransferManagerBuilder.standard().withS3Client(s3Client).build();
        MultipleFileUpload upload = manager
            .uploadDirectory(targetBucketName, s3Prefix, new File(localPath), true);
        upload.waitForCompletion();
    }

}
