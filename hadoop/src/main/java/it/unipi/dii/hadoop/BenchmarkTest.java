package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.model.BloomFilter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class BenchmarkTest {

    public static void main(String[] args) throws IOException {

        String file = "film-ratings.txt";

        int[] counts = new int[11];

        // Get count by rating
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            int roundRating;

            while ((line = br.readLine()) != null) {
                roundRating = (int) Math.round(Double.parseDouble(line.split("\t")[1]));
                counts[roundRating]++;
            }
        }

        // Init Bloom Filter
        BloomFilter[] bloomFilters = new BloomFilter[11];
        double p = 0.01;

        for(int i=0; i<=10; i++) {
            int n = counts[i];
            int m = (int) Math.round(-n*Math.log(p)/(Math.log(2)*Math.log(2)));
            int k = (int) Math.round((m*Math.log(2))/n);
            bloomFilters[i] = new BloomFilter(m,k);
        }

        // Add films to Bloom Filters
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            int roundRating;

            while ((line = br.readLine()) != null) {
                roundRating = (int) Math.round(Double.parseDouble(line.split("\t")[1]));
                bloomFilters[roundRating].add(line.split("\t")[0]);
            }
        }

        // Validate results
        int[] falsePositiveCounter = new int[11];

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            int roundRating;

            while ((line = br.readLine()) != null) {
                roundRating = (int) Math.round(Double.parseDouble(line.split("\t")[1]));

                for (int i=0; i<=10; i++) {
                    if (roundRating == i)
                        continue;
                    if (bloomFilters[i].find(line.split("\t")[0]))
                        falsePositiveCounter[i]++;
                }
            }
        }

        for(int i=0; i<=10; i++) {
            System.out.println(falsePositiveCounter[i]);
        }
    }
}
