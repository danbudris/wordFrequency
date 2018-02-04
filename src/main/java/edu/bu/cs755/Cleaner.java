package edu.bu.cs755;

import java.util.stream.*;
import java.util.*;
import java.util.function.Function;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.IOUtils;
import java.io.IOException;


public class Cleaner {
    public static void main (String[] args) throws IOException {
        Bucket pages = new Bucket("metcs755", "WikipediaPages_oneDocPerLine_1000Lines_small.txt", "test3", "test4");
        S3ObjectInputStream s3is = pages.getObjectStream();
        List <String> pageStrings = IOUtils.readLines(s3is, "UTF-8");

        //For each line in the list of pages, clean it, split it, and map it to a map with the word count as a value and the word as a key
        Map <String, Long> cleanedList = pageStrings.stream()
                .map(s -> s.replaceAll("<[^>]+>"," "))
                .map(s -> s.replaceAll("[^A-z\\s+]",""))
                .map(String::toUpperCase)
                .map(s -> s.split("\\s+"))
                .flatMap(Arrays::stream)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        //For each word/value pair in the cleaned list, sort it, get the top 5000, and map it back to a simple list of strings
        List <String> sortedList = cleanedList.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(5000)
                .map(x -> x.getKey())
                .collect(Collectors.toList());

        sortedList.forEach(System.out::println);
    }
}
