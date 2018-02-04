package edu.bu.cs755;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class Cleaner {
    public static void main(String[] args) {
        Bucket pages = new Bucket("metcs755", "WikipediaPages_oneDocPerLine_1000Lines_small.txt", "test3", "test4");
        S3ObjectInputStream s3is = pages.getObjectStream();
        /*java.util.Scanner s = new java.util.Scanner(s3is).useDelimiter("\\A");
        System.out.println(s.hasNext() ? s.next() : "");
        */
    }

}