package edu.bu.cs755;

import java.util.stream.*;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import java.io.IOException;

public class Cleaner {
    public static void main (String[] args) throws IOException {

        //Set up the connection to the bucket, and read in the file
        String bucket_name = "metcs755";
        String key_name = "WikipediaPages_oneDocPerLine_1000Lines_small.txt";
        AmazonS3 s3 = AmazonS3Client.builder().withRegion("us-east-1").build();
        S3Object o = s3.getObject(bucket_name, key_name);
        S3ObjectInputStream s3is = o.getObjectContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(s3is));

        //Count the words
        Map<String, Integer> wordCount = reader.lines().parallel()
                .map(line -> line.split(">")[1].replaceAll("<[^>]+>", "")) //
                .flatMap(line -> Arrays.stream(line.trim().split(" ")))
                .map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim())
                .filter(word -> word.length() > 0).map(word -> new SimpleEntry<>(word, 1))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (v1, v2) -> v1 + v2));

        //Generate the top 50 values
        Map<String, Integer> result = wordCount.entrySet().parallelStream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(50)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue,
                        LinkedHashMap::new));

        result.forEach((k, v) -> System.out.println(String.format("%s ->  %d", k, v)));

        //Task 1: word position
        Stream<String> searchlist = Stream.of("car", "protein", "time", "and", "during");

        searchlist.forEach(i -> {
            if (result.containsKey(i))
                System.out.println(i + " " + result.get(i));
            else
                System.out.println("-1");
                }
        );
        /*
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
                .map(s -> s.getKey())
                .collect(Collectors.toList());

        sortedList.forEach(System.out::println);
        */
    }
}
