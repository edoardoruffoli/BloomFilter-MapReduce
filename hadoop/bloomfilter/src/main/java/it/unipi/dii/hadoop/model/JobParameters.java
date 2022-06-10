package it.unipi.dii.hadoop.model;

import java.io.FileInputStream;
import java.util.Properties;

public class JobParameters {
    // Dataset
    private String inputPath;
    private String outputPath;

    // Bloom Filters
    private double p;

    // Hadoop
    private int nReducersJob0;
    private int nReducersJob1;
    private int nReducersJob2;
    private int nLineSplitJob1;
    private boolean verbose;

    public JobParameters(String path) {
        Properties prop = new Properties();

        try{
            FileInputStream fis = new FileInputStream(path);
            prop.load(fis);

            inputPath = prop.getProperty("inputPath");
            outputPath = prop.getProperty("outputPath");
            p = Double.parseDouble(prop.getProperty("p"));
            nReducersJob0 = Integer.parseInt(prop.getProperty("job0-n-reducers"));
            nReducersJob1 = Integer.parseInt(prop.getProperty("job1-n-reducers"));
            nReducersJob2 = Integer.parseInt(prop.getProperty("job2-n-reducers"));
            nLineSplitJob1 = Integer.parseInt(prop.getProperty("job1-n-line-split"));
            verbose = Boolean.parseBoolean(prop.getProperty("verbose"));
        }
        catch(Exception e){
            e.printStackTrace();
            System.exit(1);
        }
        validate();
    }

    private void validate() {
        if (p <= 0 || p > 1) {
            System.err.println("JobParameters validation error: value of p not valid");
            System.exit(1);
        }

        if (nReducersJob0 <= 0) {
            System.err.println("JobParameters validation error: value of nReducersJob0 not valid");
            System.exit(1);
        }

        if (nReducersJob1 <= 0) {
            System.err.println("JobParameters validation error: value of nReducersJob1 not valid");
            System.exit(1);
        }

        if (nReducersJob2 <= 0) {
            System.err.println("JobParameters validation error: value of nReducersJob2 not valid");
            System.exit(1);
        }

        if (nLineSplitJob1 <= 0) {
            System.err.println("JobParameters validation error: value of nLineSplitJob2 not valid");
            System.exit(1);
        }
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public double getP() {
        return p;
    }

    public int getnReducersJob0() { return nReducersJob0; }

    public int getnReducersJob1() { return nReducersJob1; }

    public int getnReducersJob2() { return nReducersJob2; }

    public int getnLineSplitJob1() { return nLineSplitJob1; }

    public boolean getVerbose() {
        return verbose;
    }
}
