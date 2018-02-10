package edu.bu.cs755;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.*;

public class Bucket {
    private String S3SourceBucket;
    private String S3SourceKey;
    private String S3DestBucket;
    private String S3DestKey;
    private String destPath;
    private String srcPath;

    public String getS3SourceBucket() {
        return this.S3SourceBucket;
    }

    public String getS3SourceKey() {
        return this.S3SourceKey;
    }

    public String getS3DestBucket() {
        return this.S3DestBucket;
    }

    public String getS3DestKey() {
        return this.S3DestKey;
    }

    public String getDestPath() {
        return this.destPath;
    }

    public String getSrcPath() {
        return this.srcPath;
    }

    public void setS3SourceBucket(String s3SourceBucket) {
        this.S3SourceBucket = s3SourceBucket;
    }

    public void setS3SourceKey(String s3SourceKey) {
        this.S3SourceKey = s3SourceKey;
    }

    public void setS3DestBucket(String s3DestBucket) {
        this.S3DestBucket = s3DestBucket;
    }

    public void setS3DestKey(String s3DestKey) {
        this.S3DestKey = s3DestKey;
    }

    public void setDestPath(String destPath) {
        this.destPath = destPath;
    }

    public void setSrcPath(String srcPath) {
        this.srcPath = srcPath;
    }

    public S3ObjectInputStream getObjectStream() {
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        S3Object o = s3.getObject(this.S3SourceBucket, this.S3SourceKey);
        S3ObjectInputStream s3is = o.getObjectContent();
        return s3is;
    }

    public Bucket(String S3SourceBucket, String S3SourceKey, String S3DestBucket, String S3DestKey) {
        this.S3SourceBucket = S3SourceBucket;
        this.S3SourceKey = S3SourceKey;
        this.S3DestBucket = S3DestBucket;
        this.S3DestKey = S3DestKey;
        this.srcPath = "https://s3.amazonaws.com/"+this.S3SourceBucket +"/" + this.S3SourceKey;
        this.destPath = this.S3DestBucket + this.S3DestKey;
    }
}