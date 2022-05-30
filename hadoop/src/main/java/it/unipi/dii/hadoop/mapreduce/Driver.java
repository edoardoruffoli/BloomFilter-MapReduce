package it.unipi.dii.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;
import java.util.Scanner;

public class Driver {

    private static int[] readFalsePositive(Configuration conf, String pathString)
            throws IOException, FileNotFoundException {
        int[] falsePositiveCounter = new int[11];
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(pathString));

        for (int i = 0; i < status.length; i++) {
            //Read the falsePositive from the hdfs
            if(!status[i].getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));

                br.lines().forEach(
                        (line)->{
                            String[] keyValueSplit = line.split("\t");
                            int rating = Integer.parseInt(keyValueSplit[0]);
                            int nFalsePositive = Integer.parseInt(keyValueSplit[1]);
                            falsePositiveCounter[rating] = nFalsePositive;
                        }
                );
                br.close();
            }
        }

        //Delete temp directory
        //hdfs.delete(new Path(pathString), true);

        return falsePositiveCounter;
    }

    private static int[] readSizes(Configuration conf, String pathString)
            throws IOException, FileNotFoundException {
        int[] sizes = new int[11];
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(pathString));

        for (int i = 0; i < status.length; i++) {
            //Read the falsePositive from the hdfs
            if (!status[i].getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));

                br.lines().forEach(
                        (line) -> {
                            String[] keyValueSplit = line.split("\t");
                            int rating = Integer.parseInt(keyValueSplit[0]);
                            int size = Integer.parseInt(keyValueSplit[1]);
                            sizes[rating] = size;
                        }
                );
                br.close();
            }
        }

        return sizes;
    }

    private static void finalize(Configuration conf, double[] falsePositiveRate, String output)
            throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream dos = hdfs.create(new Path(output + "/false-positive.txt"), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));

        //Write the result in a unique file
        for(int i = 0; i < falsePositiveRate.length; i++) {
            br.write(String.valueOf(falsePositiveRate[i]));
            br.newLine();
        }

        br.close();
        hdfs.close();
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //LocalConfiguration localConfig = new LocalConfiguration("config.ini");

        String BASE_DIR = "output" /*localConfig.getOutputPath()*/ + "/";

        conf.set("input.dataset", "film-ratings.txt"/*localConfig.getInputPath()*/);
        conf.setDouble("p", 0.01 /*localConfig.getInputPath()*/);
        conf.set("output.bloomfilter", BASE_DIR + "bloomFilters");
        conf.set("output.countByRating", BASE_DIR + "countByRating");
        conf.set("falsePositiveCount", BASE_DIR + "falsePositive");

        //conf.setBoolean("verbose", false);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(BASE_DIR), true);

        /* Parameter Calibration Stage
            - input : dataset
            - output: count by rating value
         */
        Job parameterCalibration = Job.getInstance(conf, "Parameter Calibration");
        if (!SizeEstimator.main(parameterCalibration) ) {
            fs.close();
            System.exit(1);
        }

        // Parameter Calibration
        int[] sizes = readSizes(conf, conf.get("output.countByRating"));

        for (int i=0; i<sizes.length; i++) {
            int n = sizes[i];
            int m = (int) Math.round((-n*Math.log(Double.parseDouble(conf.get("p"))))/(Math.log(2)*Math.log(2)));
            int k = (int) Math.round((m*Math.log(2))/n);

            conf.set("input.filter.m_"+i, String.valueOf(m));
            conf.set("input.filter.k_"+i, String.valueOf(k));
        }

        /* Bloom Filter Creation Stage
            - input : dataset
            - output: Bloom Filters
         */
        Job bloomFilterCreation = Job.getInstance(conf, "Bloom Filter Creation");
        if (!BloomFilterCreation.main(bloomFilterCreation) ) {
            fs.close();
            System.exit(1);
        }

        /* Parameter Validation Stage
            - input : Bloom Filters
            - output: False Positive Count
         */
        Job parameterValidation = Job.getInstance(conf, "Parameter Validation");
        if (!ParameterValidation.main(parameterValidation) ) {
            fs.close();
            System.exit(1);
        }

        int[] falsePositiveCounter = readFalsePositive(conf, conf.get("falsePositiveCount"));

        double[] falsePositiveRate = new double[11];
        int tot = 0;

        for (int i=0; i<=10; i++)
            tot += sizes[i];

        for (int i=0; i<=10; i++) {
            falsePositiveRate[i] = (double) 100*falsePositiveCounter[i]/(tot-sizes[i]);
        }

        finalize(conf, falsePositiveRate, conf.get("falsePositiveCount"));

    }
}
