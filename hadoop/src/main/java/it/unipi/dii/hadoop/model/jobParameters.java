package it.unipi.dii.hadoop.model;

import java.io.FileInputStream;
import java.util.Properties;

public class jobParameters {
    // Dataset
    private String inputPath;
    private String outputPath;

    // Bloom Filters
    private double p;

    // Hadoop
    private int nReducersJob0;
    private int nReducersJob1;
    private int nReducersJob2;
    private boolean verbose;

    public jobParameters(String path) {
        Properties prop = new Properties();

        try{
            FileInputStream fis = new FileInputStream(path);
            prop.load(fis);

            inputPath = prop.getProperty("inputPath");
            outputPath = prop.getProperty("outputPath");
            p = Double.parseDouble(prop.getProperty("p"));
            nReducersJob0 = Integer.parseInt(prop.getProperty("job0-nReducers"));
            nReducersJob1 = Integer.parseInt(prop.getProperty("job1-nReducers"));
            nReducersJob2 = Integer.parseInt(prop.getProperty("job2-nReducers"));
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
            System.err.println("LocalConfiguration validation error: value of p not valid");
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

    public boolean getVerbose() {
        return verbose;
    }
}
