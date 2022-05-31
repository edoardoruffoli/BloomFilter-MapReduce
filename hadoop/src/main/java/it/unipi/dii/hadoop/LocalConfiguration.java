package it.unipi.dii.hadoop;

import java.io.FileInputStream;
import java.util.Properties;

public class LocalConfiguration {
    // [Dataset]
    private String inputPath;
    private String outputPath;

    // [Bloom Filter]
    private double p;

    // [Hadoop]
    private boolean verbose;

    public LocalConfiguration(String path) {
        Properties prop = new Properties();

        try{
            FileInputStream fis = new FileInputStream(path);
            prop.load(fis);

            inputPath = prop.getProperty("inputPath");
            outputPath = prop.getProperty("outputPath");
            p = Double.parseDouble(prop.getProperty("p"));
            verbose = Boolean.getBoolean(prop.getProperty("verbose"));
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

    public boolean getVerbose() {
        return verbose;
    }
}
