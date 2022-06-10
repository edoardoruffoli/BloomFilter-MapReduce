package it.unipi.dii.hadoop;

import it.unipi.dii.hadoop.model.JobParameters;
import it.unipi.dii.hadoop.mapreduce.BloomFilterCreation;
import it.unipi.dii.hadoop.mapreduce.ParameterCalibration;
import it.unipi.dii.hadoop.mapreduce.ParameterValidation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;

import java.io.*;
import java.util.Arrays;

public class Driver {

    private static double[] readJobOutput(Configuration conf, String pathString) throws IOException {
        double[] tmp = new double[10];
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(pathString));

        for (FileStatus fileStatus : status) {
            if (!fileStatus.getPath().toString().endsWith("_SUCCESS")) {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(fileStatus.getPath())));

                br.lines().forEach(
                        (line) -> {
                            String[] keyValueSplit = line.split("\t");
                            int key = Integer.parseInt(keyValueSplit[0]);
                            int value = Integer.parseInt(keyValueSplit[1]);
                            tmp[key-1] = value;
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
                .replace(" ", "")
                .replace("]", "");
        String[] values = array.split(",");

        int rate = 1;
        for (String value : values) {
            br.write(rate + "\t" + value);
            br.newLine();
            rate++;
        }
        br.close();
        hdfs.close();
    }

    /**
     * Function that prints on the stdout the counters of the job specified as a parameter.
     * @param job
     * @throws IOException
     */
    private static void printJobCounters(Job job) throws IOException {
        Counters counters = job.getCounters();

        // Use > as token to split different job counters
        System.out.print(">" + job.getJobName());

        for (CounterGroup group : counters) {
            System.out.println("\t" + group.getDisplayName());
            for (Counter counter : group) {
                System.out.println("\t" + counter.getDisplayName() + ": " + counter.getValue());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // User-Specified jobs parameters
        JobParameters jobParameters = new JobParameters("config.properties");

        String BASE_DIR = jobParameters.getOutputPath() + "/";

        conf.set("input.dataset", jobParameters.getInputPath());
        conf.setDouble("input.p", jobParameters.getP());

        conf.set("output.parameter-calibration", BASE_DIR + "parameter-calibration");
        conf.set("output.bloom-filters", BASE_DIR + "bloom-filters");
        conf.set("output.parameter-validation", BASE_DIR + "parameter-validation");

        conf.setInt("job0-n-reducers", jobParameters.getnReducersJob0());
        conf.setInt("job1-n-reducers", jobParameters.getnReducersJob1());
        conf.setInt("job2-n-reducers", jobParameters.getnReducersJob2());
        conf.setInt("job1-n-line-split", jobParameters.getnLineSplitJob1());
        conf.setBoolean("verbose", jobParameters.getVerbose());

        // Clean HDFS workspace
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(BASE_DIR), true);

        // Timer
        long startTime, endTime;

        // Start execution timer
        startTime = System.currentTimeMillis();

        /* Parameter Calibration Stage
            - input : Dataset
            - output: Number of films for each rating
         */
        Job parameterCalibration = Job.getInstance(conf, "Parameter Calibration");
        if (!ParameterCalibration.main(parameterCalibration) ) {
            fs.close();
            System.exit(1);
        }
        endTime = System.currentTimeMillis();
        System.out.println("> Parameter Calibration Stage execution time: " + (endTime - startTime));
        // printJobCounters(parameterCalibration);

        // Compute and set Bloom Filter parameters based on the result of the Parameter Calibration stage
        double[] countByRating = readJobOutput(conf, conf.get("output.parameter-calibration"));

        for (int i=0; i<countByRating.length; i++) {
            double n = countByRating[i];
            int m = (int) Math.round((-n*Math.log(Double.parseDouble(conf.get("input.p"))))/(Math.log(2)*Math.log(2)));
            int k = (int) Math.round((m*Math.log(2))/n);

            conf.set("input.filter_" + i + ".m", String.valueOf(m));
            conf.set("input.filter_" + i + ".k", String.valueOf(k));
        }

        // Start execution timer
        startTime = System.currentTimeMillis();

        /* Bloom Filter Creation Stage
            - input : Dataset
            - output: Bloom Filters
         */
        Job bloomFilterCreation = Job.getInstance(conf, "Bloom Filter Creation");
        if (!BloomFilterCreation.main(bloomFilterCreation) ) {
            fs.close();
            System.exit(1);
        }
        endTime = System.currentTimeMillis();
        System.out.println("> Bloom Filter Creation Stage execution time: " + (endTime - startTime));
        // printJobCounters(bloomFilterCreation);

        // Start execution timer
        startTime = System.currentTimeMillis();
        /* Parameter Validation Stage
            - input : Bloom Filters
            - output: Number of false positives for each rating
         */
        Job parameterValidation = Job.getInstance(conf, "Parameter Validation");
        if (!ParameterValidation.main(parameterValidation) ) {
            fs.close();
            System.exit(1);
        }
        endTime = System.currentTimeMillis();
        System.out.println("> Parameter Validation Stage execution time: " + (endTime - startTime));
        // printJobCounters(parameterValidation);

        // Compute false positive rate for each rating
        double[] falsePositiveCounter = readJobOutput(conf, conf.get("output.parameter-validation"));
        double[] falsePositiveRate = new double[10];
        double tot = 0;

        for (int i=0; i<10; i++)
            tot += countByRating[i];

        for (int i=0; i<10; i++) {
            falsePositiveRate[i] = (double) 100*falsePositiveCounter[i]/(tot-countByRating[i]);
        }

        // Write the false positive rates on file
        String outputPath = conf.get("output.parameter-validation") + "/false-positive-rate.txt";
        writeJobResults(conf, Arrays.toString(falsePositiveRate), outputPath);
    }
}