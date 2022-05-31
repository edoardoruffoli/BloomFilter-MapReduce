package it.unipi.dii.hadoop.mapreduce;

import it.unipi.dii.hadoop.LocalConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;

public class Driver {

    private static int[] readFalsePositive(Configuration conf, String pathString) throws IOException {
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

    private static int[] readSizes(Configuration conf, String pathString) throws IOException {
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

        // Timers
        long start = 0, end = 0, startBC = 0, endBC = 0;

        LocalConfiguration localConfig = new LocalConfiguration("config.properties");

        String BASE_DIR = localConfig.getOutputPath() + "/";

        conf.set("input.dataset", localConfig.getInputPath());
        conf.setDouble("p", localConfig.getP());
        conf.set("output.bloomfilter", BASE_DIR + "bloom-filters");
        conf.set("output.count-by-rating", BASE_DIR + "parameter-calibration");
        conf.set("output.false-positive-count", BASE_DIR + "parameter-validation");

        conf.setBoolean("verbose", localConfig.getVerbose());

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(BASE_DIR), true);

        // Start timer
        start = System.currentTimeMillis();

        /* Parameter Calibration Stage
            - input : dataset
            - output: count by rating value
         */
        Job parameterCalibration = Job.getInstance(conf, "Parameter Calibration");
        if (!ParameterCalibration.main(parameterCalibration) ) {
            fs.close();
            System.exit(1);
        }

        // Parameter Calibration
        int[] sizes = readSizes(conf, conf.get("output.count-by-rating"));

        for (int i=0; i<sizes.length; i++) {
            int n = sizes[i];
            int m = (int) Math.round((-n*Math.log(Double.parseDouble(conf.get("p"))))/(Math.log(2)*Math.log(2)));
            int k = (int) Math.round((m*Math.log(2))/n);

            conf.set("input.filter_" + i + ".m", String.valueOf(m));
            conf.set("input.filter_" + i + ".k", String.valueOf(k));
        }

        startBC = System.currentTimeMillis();

        /* Bloom Filter Creation Stage
            - input : dataset
            - output: Bloom Filters
         */
        Job bloomFilterCreation = Job.getInstance(conf, "Bloom Filter Creation");
        if (!BloomFilterCreation.main(bloomFilterCreation) ) {
            fs.close();
            System.exit(1);
        }

        endBC = System.currentTimeMillis();

        /* Parameter Validation Stage
            - input : Bloom Filters
            - output: False Positive Count
         */
        Job parameterValidation = Job.getInstance(conf, "Parameter Validation");
        if (!ParameterValidation.main(parameterValidation) ) {
            fs.close();
            System.exit(1);
        }

        int[] falsePositiveCounter = readFalsePositive(conf, conf.get("output.false-positive-count"));

        double[] falsePositiveRate = new double[11];
        int tot = 0;

        for (int i=0; i<=10; i++)
            tot += sizes[i];

        for (int i=0; i<=10; i++) {
            falsePositiveRate[i] = (double) 100*falsePositiveCounter[i]/(tot-sizes[i]);
        }

        finalize(conf, falsePositiveRate, conf.get("output.false-positive-count"));

        end = System.currentTimeMillis();

        System.out.println("Total Job execution time: " + (end - start) + " ms");
        System.out.println("Bloom Filter Creation Job execution time: " + (endBC - startBC) + " ms");
    }
}
