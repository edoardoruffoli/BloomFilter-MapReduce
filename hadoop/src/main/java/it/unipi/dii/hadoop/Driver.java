package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.model.LocalConfiguration;
import it.unipi.dii.hadoop.mapreduce.BloomFilterCreation;
import it.unipi.dii.hadoop.mapreduce.ParameterCalibration;
import it.unipi.dii.hadoop.mapreduce.ParameterValidation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;
import java.util.Arrays;

public class Driver {

    private static double[] readJobOutput(Configuration conf, String pathString) throws IOException {
        double[] tmp = new double[11];
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(pathString));

        for (FileStatus fileStatus : status) {
            //Read the falsePositive from the hdfs
            if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(fileStatus.getPath())));

                br.lines().forEach(
                        (line) -> {
                            String[] keyValueSplit = line.split("\t");
                            int key = Integer.parseInt(keyValueSplit[0]);
                            int value = Integer.parseInt(keyValueSplit[1]);
                            tmp[key] = value;
                        }
                );
                br.close();
            }
        }
        return tmp;
    }

    private static void writeJobResults(Configuration conf, String array, String outputPath)
            throws IOException {
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream dos = hdfs.create(new Path(outputPath), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));

        // Trim the values
        array = array.replace("[", "")
             .replace("]", "");
        String[] values = array.split(",");

        for (String value : values) {
            br.write(value);
            br.newLine();
        }
        br.close();
        hdfs.close();
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Timers: general execution timer and Bloom Filter creation execution timer
        long start, end, startBC, endBC;

        // Local User-Specified configurations parameters
        LocalConfiguration localConfig = new LocalConfiguration("config.properties");

        String BASE_DIR = localConfig.getOutputPath() + "/";

        conf.set("input.dataset", localConfig.getInputPath());
        conf.setDouble("input.p", localConfig.getP());
       // conf.setInt("input.job1-num-reducer", localConfig.getNumReducer());
        //  conf.setInt("input.job1-num-reducer", localConfig.getNumReducer());
        conf.set("output.parameter-calibration", BASE_DIR + "parameter-calibration");
        conf.set("output.bloom-filters", BASE_DIR + "bloom-filters");
        conf.set("output.parameter-validation", BASE_DIR + "parameter-validation");

        conf.setBoolean("verbose", localConfig.getVerbose());

        // Clean HDFS workspace
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(BASE_DIR), true);

        // Start general execution timer
        start = System.currentTimeMillis();

        /* Parameter Calibration Stage
            - input : Dataset
            - output: Number of films for each rating
         */
        Job parameterCalibration = Job.getInstance(conf, "Parameter Calibration");
        if (!ParameterCalibration.main(parameterCalibration) ) {
            fs.close();
            System.exit(1);
        }

        // Compute and set Bloom Filter parameters based on the result of the Parameter Calibration stage
        double[] countByRating = readJobOutput(conf, conf.get("output.parameter-calibration"));

        for (int i=0; i<countByRating.length; i++) {
            double n = countByRating[i];
            int m = (int) Math.round((-n*Math.log(Double.parseDouble(conf.get("input.p"))))/(Math.log(2)*Math.log(2)));
            int k = (int) Math.round((m*Math.log(2))/n);

            conf.set("input.filter_" + i + ".m", String.valueOf(m));
            conf.set("input.filter_" + i + ".k", String.valueOf(k));
        }

        // Start Bloom Filter creation execution timer
        startBC = System.currentTimeMillis();

        /* Bloom Filter Creation Stage
            - input : Dataset
            - output: Bloom Filters
         */
        Job bloomFilterCreation = Job.getInstance(conf, "Bloom Filter Creation");
        if (!BloomFilterCreation.main(bloomFilterCreation) ) {
            fs.close();
            System.exit(1);
        }

        // Stop Bloom Filter creation execution timer
        endBC = System.currentTimeMillis();

        /* Parameter Validation Stage
            - input : Bloom Filters
            - output: Number of false positives for each rating
         */
        Job parameterValidation = Job.getInstance(conf, "Parameter Validation");
        if (!ParameterValidation.main(parameterValidation) ) {
            fs.close();
            System.exit(1);
        }

        // Write the Parameter Validation Stage output on file
        double[] falsePositiveCounter = readJobOutput(conf, conf.get("output.parameter-validation"));
        String outputPath = conf.get("output.parameter-validation") + "/false-positive-count.txt";
        writeJobResults(conf, Arrays.toString(falsePositiveCounter), outputPath);

        // Compute false positive rate for each rating
        double[] falsePositiveRate = new double[11];
        double tot = 0;

        for (int i=0; i<=10; i++)
            tot += countByRating[i];

        for (int i=0; i<=10; i++) {
            falsePositiveRate[i] = (double) 100*falsePositiveCounter[i]/(tot-countByRating[i]);
        }

        // Write the false positive rates on file
        outputPath = conf.get("output.parameter-validation") + "/false-positive-rate.txt";
        writeJobResults(conf, Arrays.toString(falsePositiveRate), outputPath);

        // Stop general execution timer
        end = System.currentTimeMillis();

        System.out.println("Total Job execution time: " + (end - start) + " ms");
        System.out.println("Bloom Filter Creation Job execution time: " + (endBC - startBC) + " ms");
    }
}
