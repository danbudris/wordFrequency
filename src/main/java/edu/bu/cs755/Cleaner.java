package edu.bu.cs755;

import java.util.stream.*;
import java.util.*;
import java.util.function.Function;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.IOUtils;
import java.io.IOException;

public class Cleaner {
    public static void main (String[] args) throws IOException {

        //Set up the connection to the bucket
        String bucket_name = "metcs755";
        String key_name = "WikipediaPages_oneDocPerLine_1000Lines_small.txt";
        AmazonS3 s3 = AmazonS3Client.builder().withRegion("us-east-1").build();
        S3Object o = s3.getObject(bucket_name, key_name);
        S3ObjectInputStream s3is = o.getObjectContent();
        List <String> pageStrings = IOUtils.readLines(s3is, "UTF-8");

        //For each line in the list of pages, clean it of metadata tags, special characters and whitespace, can convert to uppercase
        List <String> cleanedList = pageStrings.stream()
                .map(s -> s.replaceAll("<[^>]+>"," "))
                .map(s -> s.replaceAll("[^A-z\\s+]",""))
                .map(String::toUpperCase)
                .collect(Collectors.toList());

        //For each cleaned string (page), split it into words, then group the words by count, sort them by count, get the top 5000, and return just a list of the keys
        List <String> sortedList = cleanedList.stream()
                .map(s -> s.split("[\\s]+"))
                .flatMap(Arrays::stream)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(5000)
                .map(x -> x.getKey())
                .collect(Collectors.toList());

        sortedList.forEach(System.out::println);
        cleanedList.forEach(System.out::println);
    }
}
